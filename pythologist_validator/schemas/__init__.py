import json, os

# Get the directory where our schemas are located
__schema_dir = os.path.split(os.path.realpath(__file__))[0]
panel = json.loads(open(os.path.join(__schema_dir,'panel.json'),'r').read())
report = json.loads(open(os.path.join(__schema_dir,'report.json'),'r').read())
