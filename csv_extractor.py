from __future__ import absolute_import

import argparse
import logging
import re
from past.builtins import unicode
import  csv
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            dest='input',
            default='./source/*',
            help='Input file to process.'
        )
        parser.add_argument(
            '--output',
            dest='output',
            default='output/csv',
            help='Output file to write the results to.'
        )


class ExtractCSVLine(beam.PTransform):
    def expand(self, pcoll):
        return (
                pcoll
                | 'Split with delimiter' >> beam.Map(lambda w: w.split(','))
                | 'Filter Lines' >> beam.Filter(lambda x: len(x) > 4)
        )





def run(save_main_session=True):
    """main entry point"""

    opts = MyOptions()
    opts.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=opts) as pipeline:
        (
                pipeline
                | 'read lines' >> beam.io.ReadFromText(opts.input)
                | 'remove duplicates line' >> beam.Distinct()
                | ExtractCSVLine()
                | 'Write results' >> beam.io.WriteToText(opts.output)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()