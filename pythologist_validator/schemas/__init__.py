import json, os

# Get the directory where our schemas are located
#print(os.path.realpath(__file__))
__schema_dir = os.path.split(os.path.realpath(__file__))[0]
#rint(__schema_dir)
panel = json.loads(open(os.path.join(__schema_dir,'panel.json'),'r').read())
