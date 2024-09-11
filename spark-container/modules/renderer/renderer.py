from jinja2 import Template
import json


class Renderer:
    def __init__(self, json_config_files:str|list[str]):
        self.json_config_files=None
        if isinstance(json_config_files, str):
            self.json_config_files = [json_config_files]
        elif isinstance(json_config_files, list):
            for file_name in json_config_files:
                if not isinstance(file_name, str):
                    raise TypeError(f"excpected path to be string or list of paths. got {type(json_config_files)}")
            self.json_config_files = json_config_files   
        else:
            raise TypeError(f"excpected path to be string or list of paths. got {type(json_config_files)}")
        
        self.config = self.__load_config(self)
        
        def __open_file(self, file_name:str)->str:
            with open(file_name) as f:
                return f.read()
        
        def __load_json_one_config(self, file_name:str)->list[dict]:
            json_string = self.__open_file(file_name)
            return json.load(json_string)
        
        def __load_config(self)->list[dict]:
            op=[]
            for file_name in self.json_config_files:
                config = self.__load_json_one_config(file_name)
                op.append(*config)
            return op
        
        def render(your_template:str) -> str:
            temp = Template(your_template)
            concated = ''
            for c in self.config:
                concated = concated + '\n' + temp.render(**c)
            return concated