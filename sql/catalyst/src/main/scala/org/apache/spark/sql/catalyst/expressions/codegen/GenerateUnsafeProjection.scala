/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Generates a [[Projection]] that returns an [[UnsafeRow]].
 *
 * It generates the code for all the expressions, computes the total length for all the columns
 * (can be accessed via variables), and then copies the data into a scratch buffer space in the
 * form of UnsafeRow (the scratch buffer will grow as needed).
 *
 * @note The returned UnsafeRow will be pointed to a scratch buffer inside the projection.
 */
object GenerateUnsafeProjection extends CodeGenerator[Seq[Expression], UnsafeProjection] {

  /** Returns true iff we support this data type. */
  def canSupport(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case t: AtomicType => true
    case _: CalendarIntervalType => true
    case t: StructType => t.forall(field => canSupport(field.dataType))
    case t: ArrayType if canSupport(t.elementType) => true
    case MapType(kt, vt, _) if canSupport(kt) && canSupport(vt) => true
    case udt: UserDefinedType[_] => canSupport(udt.sqlType)
    case _ => false
  }

  // TODO: if the nullability of field is correct, we can use it to save null check.
  private def writeStructToBuffer(
      ctx: CodegenContext,
      input: String,
      fieldTypes: Seq[DataType],
      bufferHolder: String): String = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val fieldEvals = fieldTypes.zipWithIndex.map { case (dt, i) =>
      ExprCode("", s"$tmpInput.isNullAt($i)", ctx.getValue(tmpInput, dt, i.toString))
    }

    s"""
      final InternalRow $tmpInput = $input;
      if ($tmpInput instanceof UnsafeRow) {
        ${writeUnsafeData(ctx, s"((UnsafeRow) $tmpInput)", bufferHolder)}
      } else {
        ${writeExpressionsToBuffer(ctx, tmpInput, fieldEvals, fieldTypes, bufferHolder)}
      }
    """
  }

  private def writeExpressionsToBuffer(
      ctx: CodegenContext,
      row: String,
      inputs: Seq[ExprCode],
      inputTypes: Seq[DataType],
      bufferHolder: String,
      isTopLevel: Boolean = false): String = {
    val rowWriterClass = classOf[UnsafeRowWriter].getName
    val rowWriter = ctx.addMutableState(rowWriterClass, "rowWriter",
      v => s"$v = new $rowWriterClass($bufferHolder, ${inputs.length});")

    val resetWriter = if (isTopLevel) {
      // For top level row writer, it always writes to the beginning of the global buffer holder,
      // which means its fixed-size region always in the same position, so we don't need to call
      // `reset` to set up its fixed-size region every time.
      if (inputs.map(_.isNull).forall(_ == "false")) {
        // If all fields are not nullable, which means the null bits never changes, then we don't
        // need to clear it out every time.
        ""
      } else {
        s"$rowWriter.zeroOutNullBytes();"
      }
    } else {
      s"$rowWriter.reset();"
    }

    val writeFields = inputs.zip(inputTypes).zipWithIndex.map {
      case ((input, dataType), index) =>
        val dt = dataType match {
          case udt: UserDefinedType[_] => udt.sqlType
          case other => other
        }
        val tmpCursor = ctx.freshName("tmpCursor")

        val setNull = dt match {
          case t: DecimalType if t.precision > Decimal.MAX_LONG_DIGITS =>
            // Can't call setNullAt() for DecimalType with precision larger than 18.
            s"$rowWriter.write($index, (Decimal) null, ${t.precision}, ${t.scale});"
          case _ => s"$rowWriter.setNullAt($index);"
        }

        val writeField = dt match {
          case t: StructType =>
            s"""
              // Remember the current cursor so that we can calculate how many bytes are
              // written later.
              final int $tmpCursor = $bufferHolder.cursor;
              ${writeStructToBuffer(ctx, input.value, t.map(_.dataType), bufferHolder)}
              $rowWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
            """

          case a @ ArrayType(et, _) =>
            s"""
              // Remember the current cursor so that we can calculate how many bytes are
              // written later.
              final int $tmpCursor = $bufferHolder.cursor;
              ${writeArrayToBuffer(ctx, input.value, et, bufferHolder)}
              $rowWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
            """

          case m @ MapType(kt, vt, _) =>
            s"""
              // Remember the current cursor so that we can calculate how many bytes are
              // written later.
              final int $tmpCursor = $bufferHolder.cursor;
              ${writeMapToBuffer(ctx, input.value, kt, vt, bufferHolder)}
              $rowWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
            """

          case t: DecimalType =>
            s"$rowWriter.write($index, ${input.value}, ${t.precision}, ${t.scale});"

          case NullType => ""

          case _ => s"$rowWriter.write($index, ${input.value});"
        }

        if (input.isNull == "false") {
          s"""
            ${input.code}
            ${writeField.trim}
          """
        } else {
          s"""
            ${input.code}
            if (${input.isNull}) {
              ${setNull.trim}
            } else {
              ${writeField.trim}
            }
          """
        }
    }

    val writeFieldsCode = if (isTopLevel && (row == null || ctx.currentVars != null)) {
      // TODO: support whole stage codegen
      writeFields.mkString("\n")
    } else {
      assert(row != null, "the input row name cannot be null when generating code to write it.")
      ctx.splitExpressions(
        expressions = writeFields,
        funcName = "writeFields",
        arguments = Seq("InternalRow" -> row))
    }

    s"""
      $resetWriter
      $writeFieldsCode
    """.trim
  }

  // TODO: if the nullability of array element is correct, we can use it to save null check.
  private def writeArrayToBuffer(
      ctx: CodegenContext,
      input: String,
      elementType: DataType,
      bufferHolder: String): String = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val arrayWriterClass = classOf[UnsafeArrayWriter].getName
    val arrayWriter = ctx.addMutableState(arrayWriterClass, "arrayWriter",
      v => s"$v = new $arrayWriterClass();")
    val numElements = ctx.freshName("numElements")
    val index = ctx.freshName("index")

    val et = elementType match {
      case udt: UserDefinedType[_] => udt.sqlType
      case other => other
    }

    val jt = ctx.javaType(et)

    val elementOrOffsetSize = et match {
      case t: DecimalType if t.precision <= Decimal.MAX_LONG_DIGITS => 8
      case _ if ctx.isPrimitiveType(jt) => et.defaultSize
      case _ => 8  // we need 8 bytes to store offset and length
    }

    val tmpCursor = ctx.freshName("tmpCursor")
    val element = ctx.getValue(tmpInput, et, index)
    val writeElement = et match {
      case t: StructType =>
        s"""
          final int $tmpCursor = $bufferHolder.cursor;
          ${writeStructToBuffer(ctx, element, t.map(_.dataType), bufferHolder)}
          $arrayWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
        """

      case a @ ArrayType(et, _) =>
        s"""
          final int $tmpCursor = $bufferHolder.cursor;
          ${writeArrayToBuffer(ctx, element, et, bufferHolder)}
          $arrayWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
        """

      case m @ MapType(kt, vt, _) =>
        s"""
          final int $tmpCursor = $bufferHolder.cursor;
          ${writeMapToBuffer(ctx, element, kt, vt, bufferHolder)}
          $arrayWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
        """

      case t: DecimalType =>
        s"$arrayWriter.write($index, $element, ${t.precision}, ${t.scale});"

      case NullType => ""

      case _ => s"$arrayWriter.write($index, $element);"
    }

    val primitiveTypeName = if (ctx.isPrimitiveType(jt)) ctx.primitiveTypeName(et) else ""
    s"""
      final ArrayData $tmpInput = $input;
      if ($tmpInput instanceof UnsafeArrayData) {
        ${writeUnsafeData(ctx, s"((UnsafeArrayData) $tmpInput)", bufferHolder)}
      } else {
        final int $numElements = $tmpInput.numElements();
        $arrayWriter.initialize($bufferHolder, $numElements, $elementOrOffsetSize);

        for (int $index = 0; $index < $numElements; $index++) {
          if ($tmpInput.isNullAt($index)) {
            $arrayWriter.setNull$primitiveTypeName($index);
          } else {
            $writeElement
          }
        }
      }
    """
  }

  // TODO: if the nullability of value element is correct, we can use it to save null check.
  private def writeMapToBuffer(
      ctx: CodegenContext,
      input: String,
      keyType: DataType,
      valueType: DataType,
      bufferHolder: String): String = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val tmpCursor = ctx.freshName("tmpCursor")

    // Writes out unsafe map according to the format described in `UnsafeMapData`.
    s"""
      final MapData $tmpInput = $input;
      if ($tmpInput instanceof UnsafeMapData) {
        ${writeUnsafeData(ctx, s"((UnsafeMapData) $tmpInput)", bufferHolder)}
      } else {
        // preserve 8 bytes to write the key array numBytes later.
        $bufferHolder.grow(8);
        $bufferHolder.cursor += 8;

        // Remember the current cursor so that we can write numBytes of key array later.
        final int $tmpCursor = $bufferHolder.cursor;

        ${writeArrayToBuffer(ctx, s"$tmpInput.keyArray()", keyType, bufferHolder)}
        // Write the numBytes of key array into the first 8 bytes.
        Platform.putLong($bufferHolder.buffer, $tmpCursor - 8, $bufferHolder.cursor - $tmpCursor);

        ${writeArrayToBuffer(ctx, s"$tmpInput.valueArray()", valueType, bufferHolder)}
      }
    """
  }

  /**
   * If the input is already in unsafe format, we don't need to go through all elements/fields,
   * we can directly write it.
   */
  private def writeUnsafeData(ctx: CodegenContext, input: String, bufferHolder: String) = {
    val sizeInBytes = ctx.freshName("sizeInBytes")
    s"""
      final int $sizeInBytes = $input.getSizeInBytes();
      // grow the global buffer before writing data.
      $bufferHolder.grow($sizeInBytes);
      $input.writeToMemory($bufferHolder.buffer, $bufferHolder.cursor);
      $bufferHolder.cursor += $sizeInBytes;
    """
  }

  def createCode(
      ctx: CodegenContext,
      expressions: Seq[Expression],
      useSubexprElimination: Boolean = false): ExprCode = {
    val exprEvals = ctx.generateExpressions(expressions, useSubexprElimination)
    val exprTypes = expressions.map(_.dataType)

    val numVarLenFields = exprTypes.count {
      case dt if UnsafeRow.isFixedLength(dt) => false
      // TODO: consider large decimal and interval type
      case _ => true
    }

    val result = ctx.addMutableState("UnsafeRow", "result",
      v => s"$v = new UnsafeRow(${expressions.length});")

    val holderClass = classOf[BufferHolder].getName
    val holder = ctx.addMutableState(holderClass, "holder",
      v => s"$v = new $holderClass($result, ${numVarLenFields * 32});")

    val resetBufferHolder = if (numVarLenFields == 0) {
      ""
    } else {
      s"$holder.reset();"
    }
    val updateRowSize = if (numVarLenFields == 0) {
      ""
    } else {
      s"$result.setTotalSize($holder.totalSize());"
    }

    // Evaluate all the subexpression.
    val evalSubexpr = ctx.subexprFunctions.mkString("\n")

    val writeExpressions =
      writeExpressionsToBuffer(ctx, ctx.INPUT_ROW, exprEvals, exprTypes, holder, isTopLevel = true)

    val code =
      s"""
        $resetBufferHolder
        $evalSubexpr
        $writeExpressions
        $updateRowSize
      """
    ExprCode(code, "false", result)
  }

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  def generate(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    create(canonicalize(expressions), subexpressionEliminationEnabled)
  }

  protected def create(references: Seq[Expression]): UnsafeProjection = {
    create(references, subexpressionEliminationEnabled = false)
  }

  private def create(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    val ctx = newCodeGenContext()
    val eval = createCode(ctx, expressions, subexpressionEliminationEnabled)

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificUnsafeProjection(references);
      }

      class SpecificUnsafeProjection extends ${classOf[UnsafeProjection].getName} {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public SpecificUnsafeProjection(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        // Scala.Function1 need this
        public java.lang.Object apply(java.lang.Object row) {
          return apply((InternalRow) row);
        }

        public UnsafeRow apply(InternalRow ${ctx.INPUT_ROW}) {
          ${eval.code.trim}
          return ${eval.value};
        }

        ${ctx.declareAddedFunctions()}
      }
      """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[UnsafeProjection]
  }
}
