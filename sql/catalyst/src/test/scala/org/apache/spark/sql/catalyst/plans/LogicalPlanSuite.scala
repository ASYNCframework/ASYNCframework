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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType

/**
 * This suite is used to test [[LogicalPlan]]'s `transformUp/transformDown` plus analysis barrier
 * and make sure it can correctly skip sub-trees that have already been analyzed.
 */
class LogicalPlanSuite extends SparkFunSuite {
  private var invocationCount = 0
  private val function: PartialFunction[LogicalPlan, LogicalPlan] = {
    case p: Project =>
      invocationCount += 1
      p
  }

  private val testRelation = LocalRelation()

  test("transformUp runs on operators") {
    invocationCount = 0
    val plan = Project(Nil, testRelation)
    plan transformUp function

    assert(invocationCount === 1)

    invocationCount = 0
    plan transformDown function
    assert(invocationCount === 1)
  }

  test("transformUp runs on operators recursively") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, testRelation))
    plan transformUp function

    assert(invocationCount === 2)

    invocationCount = 0
    plan transformDown function
    assert(invocationCount === 2)
  }

  test("transformUp skips all ready resolved plans wrapped in analysis barrier") {
    invocationCount = 0
    val plan = AnalysisBarrier(Project(Nil, Project(Nil, testRelation)))
    plan transformUp function

    assert(invocationCount === 0)

    invocationCount = 0
    plan transformDown function
    assert(invocationCount === 0)
  }

  test("transformUp skips partially resolved plans wrapped in analysis barrier") {
    invocationCount = 0
    val plan1 = AnalysisBarrier(Project(Nil, testRelation))
    val plan2 = Project(Nil, plan1)
    plan2 transformUp function

    assert(invocationCount === 1)

    invocationCount = 0
    plan2 transformDown function
    assert(invocationCount === 1)
  }

  test("isStreaming") {
    val relation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())
    val incrementalRelation = LocalRelation(
      Seq(AttributeReference("a", IntegerType, nullable = true)()), isStreaming = true)

    case class TestBinaryRelation(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
      override def output: Seq[Attribute] = left.output ++ right.output
    }

    require(relation.isStreaming === false)
    require(incrementalRelation.isStreaming === true)
    assert(TestBinaryRelation(relation, relation).isStreaming === false)
    assert(TestBinaryRelation(incrementalRelation, relation).isStreaming === true)
    assert(TestBinaryRelation(relation, incrementalRelation).isStreaming === true)
    assert(TestBinaryRelation(incrementalRelation, incrementalRelation).isStreaming)
  }
}
