/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples.kotlin

import com.avro.great.TodoProvider
import org.apache.avro.protobuf.ProtobufDatumReader

object Proto2Avro {

    @JvmStatic
    fun main(args: Array<String>) {
        println("******************************************************************************")
        println("******** How to convert a proto schema to avro with 2 lines of code **********")
        println("******************************************************************************")

        println("protobuff schema description:")
        println(TodoProvider.Person.getDescriptor().toProto())
        println("----------")
        println("Avro schema:")
        val datumReader: ProtobufDatumReader<TodoProvider.Person> = ProtobufDatumReader<TodoProvider.Person>(TodoProvider.Person::class.java)
        println(datumReader.getSchema().toString(true))
    }
}
