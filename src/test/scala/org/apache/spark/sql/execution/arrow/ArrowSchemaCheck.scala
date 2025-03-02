/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.spark.sql.execution.arrow
import com.nec.spark.SparkAdditions
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

final class ArrowSchemaCheck extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {

  private def arrowSchema =
    ArrowUtils.toArrowSchema(StructType(Array(StructField("value", StringType))), "UTC")

  private def firstField = arrowSchema.getFields.get(0)

  "For a single String column from Spark" - {
    "There is only one field" in {
      assert(arrowSchema.getFields.size() == 1)
    }
    "First field's name" in {
      assert(firstField.getName == "value")
    }
    "First field is nullable" in {
      assert(firstField.isNullable)
    }
    "Has no children" in {
      assert(firstField.getChildren.size() == 0)
    }
    "Has null dictionary" in {
      assert(firstField.getDictionary == null)
    }
    "Has some field type" in {
      assert(firstField.getFieldType != null)
    }
    "The field type's .getType is the same as .getType on the field" in {
      assert(firstField.getFieldType.getType == firstField.getType)
    }
    "Has no metadata" in {
      assert(firstField.getMetadata.isEmpty)
    }
    "Is not complex" in {
      assert(!firstField.getType.isComplex)
    }
    "The type is Utf8" in {
      assert(firstField.getType == ArrowType.Utf8.INSTANCE)
    }

  }

  "The type can be reconstructed from Arrow's JSON definition schema type" in {
    info("So that we can build it from our own tests, independent of the Spark APIs")
    assert(
      arrowSchema == org.apache.arrow.vector.types.pojo.Schema.fromJSON(
        """{"fields": [{"name": "value", "nullable" : true, "type": {"name": "utf8"}, "children": []}]}"""
      )
    )
  }
}
