#!/usr/bin/env python
"""
The CLI tool for validating the setup of templates, and the staging of files.  

"""
import argparse, os, json, sys, hashlib
from importlib_resources import files
from pythologist_schemas.template import excel_to_json
from pythologist_schemas.platforms.InForm.files import injest_project, injest_sample
from pythologist_reader.formats.inform.custom import CellFrameInFormLineArea, CellFrameInFormCustomMask
from pythologist_reader.formats.inform.frame import CellFrameInForm
from pythologist_image_utilities import hash_tiff_contents
import pandas as pd


sys.setrecursionlimit(15000)

def main(args):

   total_success = True
   ## 1. Read in the set of excel templates and validate thier formats

   _fname = files('schema_data.inputs').joinpath('panel.json')
   panel_json, panel_success, panel_errors  = excel_to_json(args.analysis_excel, \
                                                              _fname,
                                                              ['Panel'], \
                                                              ignore_extra_parameters=True)

   total_success = total_success and panel_success
   
   _fname = files('schema_data.inputs.platforms.InForm').joinpath('analysis.json')
   analysis_json, analysis_success, analysis_errors  = excel_to_json(args.analysis_excel, \
                                                                       _fname,
                                                                       ['Exports','Mutually Exclusive Phenotypes','Binary Phenotypes','Regions'], \
                                                                       ignore_extra_parameters=True)

   total_success = total_success and analysis_success


   _fname = files('schema_data.inputs.platforms.InForm').joinpath('project.json')
   project_json, project_success, project_errors  = excel_to_json(args.project_excel, \
                                                                   _fname,
                                                                   ['Samples'], \
                                                                   ignore_extra_parameters=False)


   total_success = total_success and project_success

   if project_json['parameters']['project_path'] is None:
     project_json['parameters']['project_path'], _tmp = os.path.split(args.project_excel)

   ## 1b. Read in the 'not absolutely necessary for end-to-end run' report

   if args.report_excel:
      _fname = files('schema_data').joinpath('report_template.json')
      report_json, report_success1, report_errors1  = excel_to_json(args.report_excel, \
                                                                    _fname,
                                                                    ['Population Percentages','Population Densities'], \
                                                                    ignore_extra_parameters=False)
      report_success2 = True
      if report_success1:
        # Check the report to ensure that it can be used.
        report_success2 = _check_report_assumptions(report_json,)

      total_success = total_success and report_success1 and report_success2

   # 2. No we can ensure the files are properly structured

   if args.sample_name: 
      sample_file, injestion_success, injest_errors = injest_sample(args.sample_name,project_json,analysis_json)   
      sample_files = [sample_file]
   else:
      sample_files, injestion_success, injest_errors = injest_project(project_json,analysis_json)
   total_success = total_success and injestion_success

   # 3. Now we can run pythologist to get a light read on each sample.

   for sample_file in sample_files:
      _lightly_validate_sample(sample_file,analysis_json,project_json,panel_json)

   if total_success:
      print("All tests passed.")

   return

def _lightly_validate_sample(sample_file,analysis_json,project_json,panel_json):

   sample_name = sample_file['sample_name']
   to_compare = {}
   for export in sample_file['exports']:
      to_compare[export['export_name']] = _lightly_validate_export(sample_name,export,analysis_json,project_json,panel_json)

   # Now for each comparison we must traverse each image to make sure the exports have the proper concordance
   tests = {}
   for export_name in to_compare:
      for image_name in to_compare[export_name]:
         for test in to_compare[export_name][image_name]:
            if test not in tests:
               tests[test] = {}
            if image_name not in tests[test]:
               tests[test][image_name] = set()
            tests[test][image_name].add(to_compare[export_name][image_name][test])
            if len(tests[test][image_name]) > 1:
               raise ValueError("Discordant exports for image "+str(image_name)+" for "+str(test))
   #print(tests)

def _lightly_validate_export(sample_name,export,analysis_json,project_json,panel_json):
   export_name = export['export_name']
   export_path = os.path.join(project_json['parameters']['project_path'],sample_name,export_name)
   to_compare = {}
   for image_frame in export['images']:
      to_compare[image_frame['image_name']] = _lightly_validate_image_frame(image_frame,export_name,analysis_json,panel_json)

   return to_compare

def _lightly_validate_image_frame(image_frame,export_name,analysis_json,panel_json):

   # Get the conversions for the channel names
   _markers = panel_json['markers']
   _markers = dict([(x['full_name'],x['marker_name']) for x in _markers])


   # Check for the microns issue
   with open(image_frame['image_data']['cell_seg_data_txt']['file_path'],'rt') as inf:
      firstline = inf.readline()
      if 'microns' in firstline:
         raise ValueError('Detected microns instead of pixels in cell seg data')

   image_name = image_frame['image_name']

   if analysis_json['parameters']['region_annotation_strategy'] == 'GIMP_TSI':
      cfi = CellFrameInFormLineArea()
      cfi.read_raw(frame_name = image_name,
                   cell_seg_data_file = image_frame['image_data']['cell_seg_data_txt']['file_path'],
                   score_data_file = None if 'score_data_txt' not in image_frame['image_data'] else \
                                     image_frame['image_data']['score_data_txt']['file_path'],
                   tissue_seg_data_file = None,
                   binary_seg_image_file = image_frame['image_data']['binary_segs_maps_tif']['file_path'],
                   component_image_file = image_frame['image_data']['component_data_tif']['file_path'],
                   verbose=False,
                   channel_abbreviations=_markers,
                   require=True,
                   require_score=False,
                   skip_segmentation_processing=False)

      line_image = [x for x in image_frame['image_annotations'] if x['mask_label']=='TSI Line'][0]
      tumor_image = [x for x in image_frame['image_annotations'] if x['mask_label']=='Tumor'][0]
      cfi.set_line_area(line_image['file_path'],
         tumor_image['file_path'],
         steps=analysis_json['parameters']['draw_margin_width'],
         verbose=True
         )
      cdf = cfi.cdf

   elif analysis_json['parameters']['region_annotation_strategy'] == 'GIMP_CUSTOM':
      cfi = CellFrameInFormCustomMask()
      cfi.read_raw(frame_name = image_name,
                   cell_seg_data_file = image_frame['image_data']['cell_seg_data_txt']['file_path'],
                   score_data_file = None if 'score_data_txt' not in image_frame['image_data'] else \
                                     image_frame['image_data']['score_data_txt']['file_path'],
                   tissue_seg_data_file = None,
                   binary_seg_image_file = image_frame['image_data']['binary_segs_maps_tif']['file_path'],
                   component_image_file = image_frame['image_data']['component_data_tif']['file_path'],
                   verbose=False,
                   channel_abbreviations=_markers,
                   require=True,
                   require_score=False,
                   skip_segmentation_processing=False)

      custom_image = [x for x in image_frame['image_annotations'] if x['mask_label']==analysis_json['parameters']['region_annotation_custom_label']][0]
      cfi.set_area(custom_image['file_path'],
         analysis_json['parameters']['region_annotation_custom_label'],
         analysis_json['parameters']['unannotated_region_label'],
         verbose=True
         )
      cdf = cfi.cdf
   elif analysis_json['parameters']['region_annotation_strategy'] in ['NO_ANNOTATION','INFORM_ANALYSIS']:
      cfi = CellFrameInForm()
      cfi.read_raw(frame_name = image_name,
                   cell_seg_data_file = image_frame['image_data']['cell_seg_data_txt']['file_path'],
                   score_data_file = None if 'score_data_txt' not in image_frame['image_data'] else \
                                     image_frame['image_data']['score_data_txt']['file_path'],
                   tissue_seg_data_file = None,
                   binary_seg_image_file = image_frame['image_data']['binary_segs_maps_tif']['file_path'],
                   component_image_file = image_frame['image_data']['component_data_tif']['file_path'],
                   verbose=False,
                   channel_abbreviations=_markers,
                   require=True,
                   require_score=False,
                   skip_segmentation_processing=False)

      custom_image = [x for x in image_frame['image_annotations'] if x['mask_label']==analysis_json['parameters']['region_annotation_custom_label']][0]
      cfi.set_area(custom_image['file_path'],
         analysis_json['parameters']['region_annotation_custom_label'],
         analysis_json['parameters']['unannotated_region_label'],
         verbose=True
         )
      cdf = cfi.cdf
   else:
      raise ValueError("unknown annotation. you shouldn't see this with approrpiate enums")
   phenotypes = cdf.phenotypes
   binary_names = cdf.scored_names
   regions = cdf.regions

   # check phenotypes
   expected_phenotypes = [x['phenotype_name'] for x in analysis_json['mutually_exclusive_phenotypes'] if x['export_name']==export_name]
   unexpected = list(set(expected_phenotypes) - set(phenotypes))
   if len(unexpected) > 0:
      raise ValueError("Image "+str(image_name)+" in "+str(export_name)+" contained unexpected phenotype(s) not defined in the analysis "+str(unexpected))

   # check binary_names
   expected_binary_names = [x['target_name'] for x in analysis_json['binary_phenotypes'] if x['export_name']==export_name]
   unexpected = list(set(expected_binary_names) - set(binary_names))
   if len(unexpected) > 0:
      raise ValueError("Image "+str(image_name)+" in "+str(export_name)+" contained unexpected binary threshold target(s) not defined in the analysis "+str(unexpected))

   # Check regions
   expected_regions = [x['region_name'] for x in analysis_json['regions'] ]
   unexpected = list(set(expected_regions) - set(regions))
   if len(unexpected) > 0:
      raise ValueError("Image "+str(image_name)+" in "+str(export_name)+" contained unexpected region(s) "+str(unexpected))
   
   seg_data = pd.read_csv(image_frame['image_data']['cell_seg_data_txt']['file_path'],sep="\t").\
       loc[:,['Cell ID', 'Cell X Position', 'Cell Y Position']].sort_values('Cell ID').\
       apply(lambda x: tuple(x),1).tolist()
   to_compare = {
      'binary seg file difference':hash_tiff_contents(image_frame['image_data']['binary_segs_maps_tif']['file_path']),
      'cell seg data segmentation difference':hashlib.sha256(json.dumps(tuple(seg_data)).encode('utf-8')).hexdigest()
   }
   #print(to_compare)
   return to_compare

def do_inputs():
   parser = argparse.ArgumentParser(
            description = "",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
   parser.add_argument('--project_excel',metavar='ProjectExcelPath',required=True,help="The path to a excel file of a filled-in project template.")
   parser.add_argument('--analysis_excel',metavar='AnalysisExcelPath',required=True,help="The path to an excel file of a filled-in analysis template.")
   parser.add_argument('--report_excel',metavar='ReportExcelPath',help="The path to an excel file of a filled-in report template.")
   parser.add_argument('--sample_name',metavar='SampleName',help="Only check one sample.")
   parser.add_argument('--output_validation_report_json',help="Save the validation report")
   parser.add_argument('--output_run_json',help="Output a json representing this run")
   parser.add_argument('--export_run_files',help="Export the run files to a designated directory")
   parser.add_argument('--temp',help="Specify a temporary directory")
   args = parser.parse_args()
   return args

def cli():
   args = do_inputs()
   main(args)

def external_cmd(cmd):
   """function for calling program by command through a function"""
   cache_argv = sys.argv
   sys.argv = cmd
   args = do_inputs()
   main(args)
   sys.argv = cache_argv


if __name__ == "__main__":
   main(do_inputs())