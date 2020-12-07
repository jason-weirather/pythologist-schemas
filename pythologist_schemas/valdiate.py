from pythologist_schemas.template import excel_to_json
from importlib_resources import files

## 1. Read in the host of excel templates

_fname = files('schema_data.inputs').joinpath('panel.json')
panel_output, panel_success, panel_errors  = excel_to_json('example_analysis.xlsx', \
                                                                   _fname,
                                                                    ['Panel'], \
                                                                    ignore_extra_parameters=True)
_fname = files('schema_data.inputs.platforms.InForm').joinpath('analysis.json')
analysis_output, analysis_success, analysis_errors  = excel_to_json('example_analysis.xlsx', \
                                                                   _fname,
                                                                    ['Exports','Mutually Exclusive Phenotypes','Binary Phenotypes','Regions'], \
                                                                    ignore_extra_parameters=True)
_fname = files('schema_data.inputs.platforms.InForm').joinpath('project.json')
project_output, project_success, project_errors  = excel_to_json('example_project.xlsx', \
                                                                   _fname,
                                                                    ['Samples'], \
                                                                    ignore_extra_parameters=False)
_fname = files('schema_data').joinpath('report.json')
report_output, report_success, report_errors  = excel_to_json('example_report.xlsx', \
                                                                   _fname,
                                                                    ['Population Percentages','Population Densities'], \
                                                                    ignore_extra_parameters=False)


"""
Now we can perfrom the cross-checks among these

a. Panel Target Names are the only valid Binary Phenotypes
b. Analysis Export Names are the only valid such type among Mutually Exclusive Phenotypes and Binary Phenotypes
c. Analysis Mutually Exclusive Phenotypes are the the only valid such types among Population Percentages and Population Densities
d. Analysis Binary Phenotypes are the only valid such types among Population Percentages and Population Densities

And more nuanced checks
e. Panel Target Names do not end with a plus or minus sign
f. One and only one export is assigned to primary phenotyping
g. Region names are appropriately set according to the region strategy

"""


"""
2. Lets read the file structure

a. Make sure the project directy exists
b. Make sure only there are only folders among the visible files
c. Make sure the visible folders are only exactly named as those among the sample manifest
d. Make sure within each sample folder that all expected Exports exist
e. Make sure there aren't more folders burried deeper in these .. only files
f. Make sure all files present start with a prefix of the sample whose folder they are in that is followed by an underscore
g. Make sure there are not image frames that do not have a cell seg data since this is how we find what ROIs we have

"""


# a. Make sure the project directory exists
parent_directory = project_output['parameters']['project_path']
if not os.path.exists(parent_directory):
    raise ValueError('Project directory "'+str(parent_directory)+'" does not exist.')
if not os.path.isdir(parent_directory):
    raise ValueError('Project directory path "'+str(parent_directory)+'" does not a directory.')

# b. Make sure the project directories visible files are only folders

folder_names = [x for x in os.listdir(parent_directory) if x[0]!='.']
for folder_name in folder_names:
    if not os.path.isdir(os.path.join(parent_directory,folder_name)):
        raise ValueError('File present in the project directory that is not a folder '+str(folder_name))

# c. Make sure the visible folders are only exactly named as those among the sample manifest

sample_allow_list = [x['sample'] for x in project_output['samples']]

unwelcome = sorted(list(set(folder_names) - set(sample_allow_list)))

if len(unwelcome) > 0:
    raise ValueError('Folder(s) are present in the project folder that are not listed in the sample manifest '+str(unwelcome))

# d. Make sure all export folders and only those export folders are present in each sample folder

expected_export_files = [x['export_name'] for x in analysis_output['inform_exports']]
def _inspect_sample_folder(sample_path,analysis_output):
    print(sample_path)
    observed_files = [x for x in os.listdir(sample_path) if x[0]!='.']
    for observed_file in observed_files:
        if not os.path.isdir(os.path.join(sample_path,observed_file)):
            raise ValueError('File is present in the Sample folder that is not an InForm Export Folder '+str(observed_file))
    missing = sorted(list(set(expected_export_files)-set(observed_files)))
    if len(missing) > 0:
        raise ValueError('InForm Export Folder(s) are missing from the sample path "'+sample_path+'" that are expected '+str(missing))
    unwelcome = sorted(list(set(observed_files)-set(expected_export_files)))
    if len(unwelcome) > 0:
        raise ValueError('Unexpected folder(s) are present in the sample path "'+sample_path+'" '+str(unwelcome))

# e. Make sure there aren't more folders burried deeper in these .. only files

# f. Make sure all files present start with a prefix of the sample whose folder they are in that is followed by an underscore

def _inspect_export_folder(export_path,sample_name,analysis_output):
    #print(export_path)
    observed_files = [x for x in os.listdir(export_path) if x[0]!='.']
    unwanted_folders = [x for x in observed_files if os.path.isdir(os.path.join(export_path,x))]
    if len(unwanted_folders) > 0:
        raise ValueError('Unexpected folder(s) present in the export folder '+str(unwanted_folders))
    misnamed = [os.path.join(export_path,x) for x in observed_files if not x.startswith(sample_name+'_')]
    if len(misnamed) > 0:
        raise ValueError('Misnamed file(s) do not start with the expected sample name "'+str(sample_name)+'" '+str(misnamed))
    # Now look for individual frames
    prog = re.compile('^(.+)_cell_seg_data\.txt$')
    frame_names = [prog.match(x).group(1) for x in observed_files if prog.match(x)]

    # g. Make sure there are not image frames that do not have a cell seg data
    all_files = set(observed_files.copy())
    for file_name in list(all_files):
        for frame_name in frame_names:
            if file_name.startswith(frame_name):
                all_files-=set([file_name])
    if len(all_files) > 0:
        raise ValueError('It appears there may be some image ROIs that are missing their cell_seg_data.txt file '+str(all_files))
        
for sample_path, sample_name in [(os.path.join(parent_directory,x),x) for x in folder_names]:
    _inspect_sample_folder(sample_path,analysis_output)
    for export_path in [os.path.join(sample_path,x) for x in expected_export_files]:
        _inspect_export_folder(export_path,sample_name,analysis_output)

_fname = files('schema_data.inputs.platforms.Inputs').joinpath('files.json')
_validator = get_validator(_fname)
        
# Now for each sample read it into the files json-schema object
def _do_export_images(export_path,sample_name,files_schema):
    # get the image_file list
    image_files = [x for x in os.listdir(export_path) if x[0]!='.']
    # get our 
    #prog = re.compile('^(.+)_cell_seg_data\.txt$')
    #cell_segs =  dict([(prog.match(x).group(1),x) for x in image_files if prog.match(x)])
    #print(cell_segs)
    image_obj = {}
    for image_file_name in files_schema['definitions']['image_data']['properties']:
        image_file = files_schema['definitions']['image_data']['properties'][image_file_name]
        suffix = image_file['properties']['suffix']['const']
        rgxstr = suffix.replace('.','\.')
        prog = re.compile('^(.+)_'+rgxstr+'$')
        _dict = dict([(prog.match(x).group(1),x) for x in image_files if prog.match(x)])
        for image_name in _dict.keys():
            if image_name not in image_obj:
                image_obj[image_name] = {}
            image_obj[image_name][image_file_name] = _dict[image_name]
    image_output = []
    for image_name in sorted(list(image_obj.keys())):
        print(image_name)
        _img = {
            'image_name':image_name,
            'image_data':{},
            'image_annotations':[]
        }
        for image_file_name in image_obj[image_name]:
            _img['image_data'][image_file_name] = {
                'file_path': os.path.join(export_path,image_obj[image_name][image_file_name])
            }
        #print(_img)
        image_output.append(_img)
    return image_output

for sample_path, sample_name in [(os.path.join(parent_directory,x),x) for x in folder_names]:
    print(sample_name)
    sample_files = {
        'sample_name':sample_name,
        'exports':[
        ]
    }
    for export_path, export_name in [(os.path.join(sample_path,x),x) for x in expected_export_files]:
        export = {
            'export_name':export_name,
            'images':_do_export_images(export_path,sample_name,_validator.schema)
        }
        sample_files['exports'].append(export)
    print(json.dumps(sample_files,indent=2))
    print(_validator.validate(instance=sample_files))
