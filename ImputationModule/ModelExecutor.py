# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 21:52:37 2020

@author: SIM
"""

import json
import ImputationModule.ModelGenerator

class ModelExecutor:
    
    def __init__(self, TrainLog, Key, Variable, Path):
        
        self.Path = Path
        
        self.TrainLog = TrainLog
        
        self.Key = Key
        
        self.Variable = Variable
        
    def FitModel(self):
        
        Model = ImputationModule.ModelGenerator.ModelGenerator(self.TrainLog, self.Key, self.Variable)
        
        RelationModel = Model.RelationModel()

        ImputeModel = Model.ImputeModel()
        
        self.FittingModel = {'Relation': RelationModel, 'Imputation': ImputeModel}
        
        return self.FittingModel
        
    def SaveModel(self):
        
        with open((self.Path+'\\EventImputationModel.json'), 'w', encoding='utf-8') as make_file:

            json.dump(self.FittingModel, make_file, indent="\t")
       
    def ReadModel(self):
        
        with open((self.Path+'\\EventImputationModel.json')) as f:

            self.FittingModel = json.load(f)
                
        return self.FittingModel
        
        