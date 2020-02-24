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
package org.apache.beam.examples.kotlin.complete

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.gcp.util.Transport
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.Validation
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.SerializableComparator
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.transforms.Top
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.CalendarWindows
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.Sessions
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ComparisonChain
import org.joda.time.Duration
import org.joda.time.Instant

/**
 * An example that reads Wikipedia edit data from Cloud Storage and computes the user , the
 * longest string of edits separated by no more than an hour within each month.
 *
 * Concepts: Using Windowing to perform time-based aggregations of data.
 */
object TopWikipediaSessions {
    private const val EXPORTED_WIKI_TABLE = "gs://apache-beam-samples/wikipedia_edits/*.json"
    private const val SAMPLING_THRESHOLD = 0.1

    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory
            .fromArgs(*args)
            .withValidation()
            as Options

        run(options)
    }

    @JvmStatic
    fun run(options: Options) {
        val pipeline = Pipeline.create(options)

        pipeline
            .apply(TextIO.read().from(options.wikiInput))
            .apply(MapElements.via<String, TableRow>(ParseTableRowJson()))
            .apply(ComputeTopSessions(options.samplingThreshold))
            .apply("Write", TextIO.write().to(options.output))

        pipeline.run().waitUntilFinish()
    }

    interface Options : PipelineOptions {
        @get:Description("Input specified as a GCS path containing a BigQuery table exported as json")
        @get:Default.String(EXPORTED_WIKI_TABLE)
        var wikiInput: String

        @get:Description("Sampling threshold for number of users")
        @get:Default.Double(SAMPLING_THRESHOLD)
        var samplingThreshold: Double

        @get:Description("File to output results to")
        @get:Validation.Required
        var output: String
    }

    class ParseTableRowJson : SimpleFunction<String, TableRow>() {
        override fun apply(input: String): TableRow =
            try {
                Transport.getJsonFactory().fromString(input, TableRow::class.java)
            } catch (e: java.io.IOException) {
                throw RuntimeException("Failed parsing table row json", e)
            }
    }

    /** Extracts user and timestamp from a TableRow representing a Wikipedia edit. */
    class ExtractUserAndTimestamp : DoFn<TableRow, String>() {
        @ProcessElement
        fun processElement(ctx: ProcessContext) {
            val row: TableRow = ctx.element()
            val timestamp: Int = when (val ts = row.get("timestamp")) {
                is java.math.BigDecimal -> ts.toBigInteger().intValueExact()
                else -> ts as Int
            }
            val userName = row.get("contributor_username") as? String
            if (userName != null) {
                // Sets the implicit timestamp field to be used in windowing.
                ctx.outputWithTimestamp(userName, Instant(timestamp * 1000L))
            }
        }
    }

    /**
     * Computes the number of edits in each user session. A session is defined as a string of edits
     * where each is separated from the next by less than an hour.
     */
    class ComputeSessions : PTransform<PCollection<String>, PCollection<KV<String, Long>>>() {
        override fun expand(actions: PCollection<String>): PCollection<KV<String, Long>> =
            actions
                .apply(Window.into<String>(Sessions.withGapDuration(Duration.standardHours(1))))
                .apply(Count.perElement())
    }

    class SessionLengthComparator : SerializableComparator<KV<String, Long>> {
        override fun compare(thiz: KV<String, Long>, that: KV<String, Long>): Int =
            ComparisonChain
                .start()
                .compare(thiz.getValue(), that.getValue())
                .compare(thiz.getKey().toString(), that.getKey().toString())
                .result()
    }

    /** Computes the longest session ending in each month. */
    class TopPerMonth : PTransform<PCollection<KV<String, Long>>, PCollection<List<KV<String, Long>>>>() {
        override fun expand(sessions: PCollection<KV<String, Long>>): PCollection<List<KV<String, Long>>> {
            val comparator = SessionLengthComparator()

            return sessions
                .apply(Window.into<KV<String, Long>>(CalendarWindows.months(1)))
                .apply(Top.of(1, comparator).withoutDefaults())
        }
    }

    class SessionsToStringsDoFn : DoFn<KV<String, Long>, KV<String, Long>>() {
        @ProcessElement
        fun processElement(ctx: ProcessContext, window: BoundedWindow): Unit =
            ctx.output(KV.of("${ctx.element().getKey()} : $window", ctx.element().getValue()))
    }

    class FormatOutputDoFn : DoFn<List<KV<String, Long>>, String>() {
        @ProcessElement
        fun processElement(ctx: ProcessContext, window: BoundedWindow) {
            for (item in ctx.element()) {
                val session = item.getKey()
                val count = item.getValue()
                val instantTs = (window as IntervalWindow).start()
                ctx.output("$session : $count : $instantTs")
            }
        }
    }

    class ComputeTopSessions(val samplingThreshold: Double) :
        PTransform<PCollection<TableRow>, PCollection<String>>() {
        override fun expand(input: PCollection<TableRow>): PCollection<String> =
            input
                .apply(ParDo.of(ExtractUserAndTimestamp()))
                .apply("SampleUsers", ParDo.of(SampleUsersFn(samplingThreshold)))
                .apply(ComputeSessions())
                .apply("SessionsToStrings", ParDo.of(SessionsToStringsDoFn()))
                .apply(TopPerMonth())
                .apply("FormatOutput", ParDo.of(FormatOutputDoFn()))
    }

    class SampleUsersFn(val samplingThreshold: Double) : DoFn<String, String>() {
        @ProcessElement
        fun processElement(ctx: ProcessContext): Unit =
            if (Math.abs(ctx.element().hashCode().toLong())
                <= Integer.MAX_VALUE * samplingThreshold
            ) {
                ctx.output(ctx.element())
            } else {
            }
    }
}
