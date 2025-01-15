# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 17:17:29 2020

@author: SIM
"""

#%%
import pandas as pd
import numpy as np
import os

# Current path
os.chdir('Current Path')

from ImputationModule import ModelExecutor
import ImputationModule.ImputationExecutor
import UtilizationModule.LogSplit
import UtilizationModule.LogGenerateForCC
import UtilizationModule.MissingEventGenerate

import warnings
warnings.filterwarnings('ignore')

FilePath = 'Data/BPI2012'

FileName = 'BPI2012.csv'

Train_path = '\Data\BPI2012\TrainLog.csv'   # Change this to TrainLog path

TestPath = '\Data\BPI2012\TestLog.csv' # Change this to TestLog path

Key = 'case:concept:name'

Variable=['Activity', 'Variant', 'Variant index', 'AMOUNT_REQ', 'concept:name','lifecycle:transition']

MissingVariable=['Activity', 'Variant', 'Variant index', 'AMOUNT_REQ','lifecycle:transition']

TimeVariable = ['Complete Timestamp']

TimeFormat = '%Y-%m-%d %H:%M'

BootstrapIteration = 1 

Iteration1 = 3

Iteration2 = 10 
                             
BoostrapingParameter = 0.8

TrainLog = pd.read_csv(Train_path)
TestLog = pd.read_csv(TestPath)




Model = ImputationModule.ModelExecutor.ModelExecutor(TrainLog, Key, Variable, Path = FilePath)

ImputationModel = Model.FitModel()

Model.SaveModel()

MissingRate = [0.30,0.35,0.40,0.45,0.5,0.55,0.60]

for n in range(len(MissingRate)):

    MissingLog = UtilizationModule.MissingEventGenerate.MissingEventGenerate(FilePath, TestLog, MissingVariable, MissingRate[n])

    TestLogWithMissing = MissingLog.MissingEventGenerate()
    
    print('Missing Rate is ', MissingRate[n])
    
    iter0 = 0

    while(iter0!=15):
    
        EventLog = TestLogWithMissing.copy()
            
        Repair = ImputationModule.ImputationExecutor.ImputationExecutor(EventLog, ImputationModel, Key, 
                                                          
                                 Variable, TimeVariable, TimeFormat, BootstrapIteration, Iteration1, Iteration2, 
                                 
                                 BoostrapingParameter)
        
        Result = Repair.RecurrentEventImputation()

        Result0 = pd.DataFrame(np.zeros((len(Variable),2)),columns = ['Variable','Acc'])
    
        for i in range(len(MissingVariable)):
            
            Result0['Variable'][i] = MissingVariable[i]
         
        for i in range(len(MissingVariable)):
            
            Result0['Acc'][i] = (1-round(sum(
                    
                    Result[0][MissingVariable[i]] != TestLog[MissingVariable[i]])/(len(TestLog)*MissingRate[n]),4))*100
                
        print(Result0)
        
        filename = (FilePath + '/Repair Accuracy Test/' + str(n+1) + '.' + str(int(MissingRate[n]*100)) + 
        
                    '%/Result' + str(iter0) + '.csv') 
        
        filename1 = FilePath + '/Repair Accuracy Test/' + str(n+1) + '.' + str(int(MissingRate[n]*100))  + '%/RepairLog.csv'
        
        Result0.to_csv(filename, header=True, index = False)
        
        Result[0].to_csv(filename1, header=True, index = False)
        
        iter0 += 1
        
    LogForCC = TestLogWithMissing.copy()
    
    Log = UtilizationModule.LogGenerateForCC.LogGenerateForCC(FilePath,LogForCC, Variable[0], MissingRate[n])
    
    Log.LogGenerateForCC()
    
# %%
