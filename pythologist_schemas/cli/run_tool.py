#!/usr/bin/env python

""" Run the pipeline and extract data.

Input is a json prepared by the staging tool.
"""

from importlib_resources import files
from pythologist_schemas import get_validator
from pythologist_reader.formats.inform import read_standard_format_sample_to_project
from pythologist import CellDataFrame, SubsetLogic as SL, PercentageLogic as PL
import logging, argparse, json, uuid

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

    for sample_input_json in inputs['sample_files']:
        sample_output_json = execute_sample(sample_input_json,inputs,run_id,verbose=args.verbose)
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


    cnts = cdf.counts()

    logger.info("frame-level densities")
    fcnts = cnts.frame_counts(subsets=density_populations)
    logger.info("sample-level densities")
    scnts = cnts.sample_counts(subsets=density_populations)

    logger.info("frame-level percentages")
    fpcnts = cnts.frame_percentages(percentage_logic_list=percentage_populations)
    logger.info("sample-level percentages")
    spcnts = cnts.sample_percentages(percentage_logic_list=percentage_populations)

    print(fpcnts.columns.tolist())
    return

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