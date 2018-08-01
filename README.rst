
export PYTHONPATH='.:/PATH/TO/CUSTOM/HANDLER/DIRECTORY/'
export CustomHandlerModule=es_handler
export CustomHandlerClass=CustomElasticFeeder
mongo-connector -m localhost:12345 -t bolt://localhost:7687 -d Custom_Adapter