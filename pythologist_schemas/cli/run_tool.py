#!/usr/bin/env python

""" Run the pipeline and extract data.

Input is a json prepared by the staging tool.
"""

from importlib_resources import files
from pythologist_schemas import get_validator
from pythologist_reader.formats.inform import read_standard_format_sample_to_project
from pythologist import CellDataFrame, SubsetLogic as SL, PercentageLogic as PL
import logging, argparse, json, uuid
from collections import OrderedDict
import pandas as pd
from datetime import datetime


def cli():
    args = do_inputs()
    main(args)

def main(args):
    "We need to take the platform and return an appropriate input template"
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger("start run")
    run_id = str(uuid.uuid4())
    logger.info("run_id "+run_id)

    inputs = json.loads(open(args.input_json,'rt').read())

    # Lets start by checking our inputs
    logger.info("check project json format")
    get_validator(files('schema_data.inputs.platforms.InForm').joinpath('project.json')).\
        validate(inputs['project'])
    logger.info("check analysis json format")
    get_validator(files('schema_data.inputs.platforms.InForm').joinpath('analysis.json')).\
        validate(inputs['analysis'])
    logger.info("check report json format")
    get_validator(files('schema_data.inputs').joinpath('report.json')).\
        validate(inputs['report'])
    logger.info("check panel json format")
    get_validator(files('schema_data.inputs').joinpath('panel.json')).\
        validate(inputs['panel'])
    _validator = get_validator(files('schema_data.inputs.platforms.InForm').joinpath('files.json'))
    for sample_input_json in inputs['sample_files']:
        logger.info("check sample files json format "+str(sample_input_json['sample_name']))
        _validator.validate(sample_input_json)

    # Now lets step through sample-by-sample executing the pipeline
    output = {
        'run_id':run_id,
        'time':str(datetime.now()),
        'project_name':inputs['project']['parameters']['project_name'],
        'report_name':inputs['report']['parameters']['report_name'],
        'report_version':inputs['report']['parameters']['report_version'],
        'analysis_name':inputs['analysis']['parameters']['analysis_name'],
        'analysis_version':inputs['analysis']['parameters']['analysis_version'],
        'panel_name':inputs['panel']['parameters']['panel_name'],
        'panel_version':inputs['panel']['parameters']['panel_version'],
        'sample_outputs':[execute_sample(x,inputs,run_id,verbose=args.verbose) for x in inputs['sample_files']]
    }
    logger.info("Finished reading creating output. Validate output format.")
    _validator = get_validator(files('schema_data').joinpath('report_output.json'))
    _validator.validate(output)
    logger.info("Validated output schema against schema")
    if args.output_json:
        with open(args.output_json,'wt') as of:
            of.write(json.dumps(output,indent=2,allow_nan=False))
    return 

def execute_sample(files_json,inputs,run_id,temp_dir=None,verbose=False):
    logger = logging.getLogger(str(files_json['sample_name']))
    logger.info("staging channel abbreviations")
    channel_abbreviations = dict([(x['full_name'],x['marker_name']) for x in inputs['panel']['markers']])
    logger.info("reading exports to temporary h5")
    exports = read_standard_format_sample_to_project(files_json['sample_directory'],
                                                     inputs['analysis']['parameters']['region_annotation_strategy'],
                                                     channel_abbreviations = channel_abbreviations,
                                                     sample = files_json['sample_name'],
                                                     project_name = inputs['project']['parameters']['project_name'],
                                                     custom_mask_name = inputs['analysis']['parameters']['region_annotation_custom_label'],
                                                     other_mask_name = inputs['analysis']['parameters']['unannotated_region_label'],
                                                     microns_per_pixel = inputs['project']['parameters']['microns_per_pixel'],
                                                     line_pixel_steps = inputs['analysis']['parameters']['draw_margin_width'],
                                                     verbose = False
        )

    for export_name in exports:
        logger.info("extract CellDataFrame from h5 objects "+str(export_name))
        exports[export_name] = exports[export_name].cdf
        exports[export_name]['project_id'] = run_id # force them to have the same project_id
        exports[export_name]['project_name'] = inputs['project']['parameters']['project_name']
        meps = [x['phenotype_name'] for x in inputs['analysis']['mutually_exclusive_phenotypes'] if x['export_name']==export_name and \
                                                                                                    x['convert_to_binary']]
        if len(meps) > 0:
            logger.info("converting mutually exclusive phenotype to binary phenotype for "+str(meps))
            exports[export_name] = exports[export_name].phenotypes_to_scored(phenotypes=meps,overwrite=False)

    logger.info("getting the primary export")
    primary_export_name = [x['export_name'] for x in inputs['analysis']['inform_exports'] if x['primary_phenotyping']]
    if len(primary_export_name) != 1: raise ValueError("didnt find the 1 single expected primary phenotyping in analysis")
    primary_export_name = primary_export_name[0]
    
    cdf = exports[primary_export_name]

    for export_name in [x for x in exports if x!=primary_export_name]:
        logger.info("merging in "+str(export_name))
        _cdf = exports[export_name]
        _cdf['project_id'] = run_id
        cdf,f = cdf.merge_scores(_cdf,on=['project_name','sample_name','frame_name','x','y','cell_index'])
        if f.shape[0] > 0:
            raise ValueError("segmentation mismatch error "+str(f.shape[0]))
    logger.info("merging completed")
    # Now cdf contains a CellDataFrame sutiable for data extraction

    # For density measurements build our population definitions
    density_populations = []
    for population in inputs['report']['population_densities']:
        _pop = SL(phenotypes = population['mutually_exclusive_phenotypes'], 
                  scored_calls = dict([(x['target_name'],x['filter_direction']) for x in population['binary_phenotypes']]),
                  label = population['population_name']
                 )
        density_populations.append(_pop)

    percentage_populations = []
    for population in inputs['report']['population_percentages']:
        _numerator = SL(phenotypes = population['numerator_mutually_exclusive_phenotypes'], 
                        scored_calls = dict([(x['target_name'],x['filter_direction']) for x in population['numerator_binary_phenotypes']])
                       )
        _denominator = SL(phenotypes = population['denominator_mutually_exclusive_phenotypes'], 
                          scored_calls = dict([(x['target_name'],x['filter_direction']) for x in population['denominator_binary_phenotypes']])
                          )
        _pop = PL(numerator = _numerator,
                  denominator = _denominator,
                  label = population['population_name']
                 )
        percentage_populations.append(_pop)

    # Now calculate outputs for each region we are working with
    fcnts = []
    scnts = []
    fpcnts = []
    spcnts = []
    for report_region_row in inputs['report']['region_selection']:
        # Iterate over each region combining them to the name specified
        report_region_name = report_region_row['report_region_name']
        regions_to_combine = report_region_row['regions_to_combine']
        logger.info("extracting data for region '"+str(report_region_name)+"' which is made up of "+str(regions_to_combine))
        _cdf = cdf.combine_regions(regions_to_combine,report_region_name)
        # Fetch counts based on qc constraints
        _cnts = _cdf.counts(minimum_region_size_pixels=inputs['report']['parameters']['minimum_density_region_size_pixels'],
                           minimum_denominator_count=inputs['report']['parameters']['minimum_denominator_count'])
        logger.info("frame-level densities")
        _fcnts = _cnts.frame_counts(subsets=density_populations)
        _fcnts = _fcnts.loc[_fcnts['region_label']==report_region_name,:]
        fcnts.append(_fcnts)
        logger.info("sample-level densities")
        _scnts = _cnts.sample_counts(subsets=density_populations)
        _scnts = _scnts.loc[_scnts['region_label']==report_region_name,:]
        scnts.append(_scnts)


        logger.info("frame-level percentages")
        _fpcnts = _cnts.frame_percentages(percentage_logic_list=percentage_populations)
        _fpcnts = _fpcnts.loc[_fpcnts['region_label']==report_region_name,:]
        fpcnts.append(_fpcnts)
        logger.info("sample-level percentages")
        _spcnts = _cnts.sample_percentages(percentage_logic_list=percentage_populations)
        _spcnts = _spcnts.loc[_spcnts['region_label']==report_region_name,:]
        spcnts.append(_spcnts)

    fcnts = pd.concat(fcnts).reset_index(drop=True)
    scnts = pd.concat(scnts).reset_index(drop=True)
    fpcnts = pd.concat(fpcnts).reset_index(drop=True)
    spcnts = pd.concat(spcnts).reset_index(drop=True)

    #prepare an output json 
    output = {
        "sample_name":files_json['sample_name'],
        "sample_reports":{
            'sample_cumulative_count_densities':[],
            'sample_aggregate_count_densities':[],
            'sample_cumulative_count_percentages':[],
            'sample_aggregate_count_percentages':[]
        },
        "images":[]
    }

    # Now fill in the data
    for image_name in [x['image_name'] for x in files_json['exports'][0]['images']]:
        output['images'].append({
            'image_name':image_name,
            'image_reports':{
                'image_count_densities':_organize_frame_count_densities(fcnts,inputs['report']['parameters']['minimum_density_region_size_pixels']),
                'image_count_percentages':_organize_frame_percentages(fpcnts,inputs['report']['parameters']['minimum_denominator_count'])
            }
        })

    # Do sample level densities
    output['sample_reports']['sample_cumulative_count_densities'] = \
        _organize_sample_cumulative_count_densities(scnts,inputs['report']['parameters']['minimum_density_region_size_pixels'])
    output['sample_reports']['sample_aggregate_count_densities'] = \
        _organize_sample_aggregate_count_densities(scnts,inputs['report']['parameters']['minimum_density_region_size_pixels'])

    # Now do percentages
    output['sample_reports']['sample_cumulative_count_percentages'] = \
        _organize_sample_cumulative_percentages(spcnts,inputs['report']['parameters']['minimum_density_region_size_pixels'])
    output['sample_reports']['sample_aggregate_count_percentages'] = \
        _organize_sample_aggregate_percentages(spcnts,inputs['report']['parameters']['minimum_density_region_size_pixels'])


    return output

def _organize_frame_percentages(frame_percentages,min_denominator_count):
    # Make the list of sample count density features in dictionary format

    # make an object to convert pythologist internal count reports to the expected column names
    conv = OrderedDict({
        'region_label':'region_name',
        'phenotype_label':'population_name',
        'numerator':'numerator_count',
        'denominator':'denominator_count',
        'fraction':'fraction',
        'percent':'percent'
    })
    keeper_columns = list(conv.values())
    frame_report = frame_percentages.rename(columns=conv).loc[:,keeper_columns]
    frame_report['measure_qc_pass'] = True
    frame_report.loc[frame_report['denominator_count'] < min_denominator_count,'measure_qc_pass'] = False

    frame_report = frame_report.where(pd.notnull(frame_report), None)
    return [row.to_dict() for index,row in frame_report.iterrows()]

def _organize_frame_count_densities(frame_count_densities,min_pixel_count):
    # Make the list of sample count density features in dictionary format

    # make an object to convert pythologist internal count reports to the expected column names
    conv = OrderedDict({
        'region_label':'region_name',
        'phenotype_label':'population_name',
        'region_area_pixels':'region_area_pixels',
        'region_area_mm2':'region_area_mm2',
        'count':'count',
        'density_mm2':'density_mm2'
    })
    keeper_columns = list(conv.values())
    #print(keeper_columns)
    #print(frame_count_densities.rename(columns=conv).columns)
    frame_report = frame_count_densities.rename(columns=conv).loc[:,keeper_columns]
    frame_report['measure_qc_pass'] = True
    frame_report.loc[frame_report['region_area_pixels'] < min_pixel_count,'measure_qc_pass'] = False

    frame_report = frame_report.where(pd.notnull(frame_report), None)
    return [row.to_dict() for index,row in frame_report.iterrows()]
def _organize_sample_cumulative_count_densities(sample_count_densities,min_pixel_count):
    # Make the list of sample count density features in dictionary format

    # make an object to convert pythologist internal count reports to the expected column names
    conv = OrderedDict({
        'region_label':'region_name',
        'phenotype_label':'population_name',
        'frame_count':'image_count',
        'cumulative_region_area_pixels':'cumulative_region_area_pixels',
        'cumulative_region_area_mm2':'cumulative_region_area_mm2',
        'cumulative_count':'cumulative_count',
        'cumulative_density_mm2':'cumulative_density_mm2'
    })
    keeper_columns = list(conv.values())

    sample_report = sample_count_densities.rename(columns=conv).loc[:,keeper_columns]
    sample_report['measure_qc_pass'] = True
    sample_report.loc[sample_report['cumulative_region_area_pixels'] < min_pixel_count,'measure_qc_pass'] = False
    
    sample_report = sample_report.where(pd.notnull(sample_report), None)
    return [row.to_dict() for index,row in sample_report.iterrows()]

def _organize_sample_aggregate_count_densities(sample_count_densities,min_pixel_count):
    # Make the list of sample count density features in dictionary format

    # make an object to convert pythologist internal count reports to the expected column names
    conv = OrderedDict({
        'region_label':'region_name',
        'phenotype_label':'population_name',
        'frame_count':'image_count',
        'measured_frame_count':'aggregate_measured_image_count',
        'mean_density_mm2':'aggregate_mean_density_mm2',
        'stddev_density_mm2':'aggregate_stddev_density_mm2',
        'stderr_density_mm2':'aggregate_stderr_density_mm2'
    })
    keeper_columns = list(conv.values())

    sample_report = sample_count_densities.rename(columns=conv).loc[:,keeper_columns]
    sample_report['measure_qc_pass'] = True
    sample_report.loc[sample_report['aggregate_measured_image_count'] < 1,'measure_qc_pass'] = False
    
    sample_report = sample_report.where(pd.notnull(sample_report), None)
    return [row.to_dict() for index,row in sample_report.iterrows()]


def _organize_sample_cumulative_percentages(sample_count_densities,min_denominator_count):
    # Make the list of sample count density features in dictionary format

    # make an object to convert pythologist internal count reports to the expected column names
    conv = OrderedDict({
        'region_label':'region_name',
        'phenotype_label':'population_name',
        'frame_count':'image_count',
        'cumulative_numerator':'cumulative_numerator_count',
        'cumulative_denominator':'cumulative_denominator_count',
        'cumulative_fraction':'cumulative_fraction',
        'cumulative_percent':'cumulative_percent'
    })
    keeper_columns = list(conv.values())

    sample_report = sample_count_densities.rename(columns=conv).loc[:,keeper_columns]
    sample_report['measure_qc_pass'] = True
    sample_report.loc[sample_report['cumulative_denominator_count'] < min_denominator_count,'measure_qc_pass'] = False
    
    sample_report = sample_report.where(pd.notnull(sample_report), None)
    return [row.to_dict() for index,row in sample_report.iterrows()]

def _organize_sample_aggregate_percentages(sample_count_densities,min_denominator_count):
    # Make the list of sample count density features in dictionary format

    # make an object to convert pythologist internal count reports to the expected column names
    conv = OrderedDict({
        'region_label':'region_name',
        'phenotype_label':'population_name',
        'frame_count':'image_count',
        'measured_frame_count':'aggregate_measured_image_count',
        'mean_fraction':'aggregate_mean_fraction',
        'stdev_fraction':'aggregate_stddev_fraction',
        'stderr_fraction':'aggregate_stderr_fraction',
        'mean_percent':'aggregate_mean_percent',
        'stdev_percent':'aggregate_stddev_percent',
        'stderr_percent':'aggregate_stderr_percent'
    })
    keeper_columns = list(conv.values())

    sample_report = sample_count_densities.rename(columns=conv).loc[:,keeper_columns]
    sample_report['measure_qc_pass'] = True
    sample_report.loc[sample_report['aggregate_measured_image_count'] < 1,'measure_qc_pass'] = False
    
    sample_report = sample_report.where(pd.notnull(sample_report), None)
    return [row.to_dict() for index,row in sample_report.iterrows()]


def do_inputs():
    parser = argparse.ArgumentParser(
            description = "Run the pipeline",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input_json',required=True,help="The json file defining the run")
    parser.add_argument('--output_json',help="The output of the pipeline")
    parser.add_argument('--verbose',action='store_true',help="Show more about the run")
    args = parser.parse_args()
    return args

def external_cmd(cmd):
    """function for calling program by command through a function"""
    cache_argv = sys.argv
    sys.argv = cmd
    args = do_inputs()
    main(args)
    sys.argv = cache_argv


if __name__ == "__main__":
    main(do_inputs())