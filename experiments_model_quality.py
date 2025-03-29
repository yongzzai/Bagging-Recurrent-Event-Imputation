# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from UtilizationModule import MissingEventGenerate as meg
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.evaluation.replay_fitness import algorithm as replay_fitness_evaluator
from pm4py.algo.evaluation.precision import algorithm as precision_evaluator
from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator
from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator

MissingRate = [0.30,0.35,0.40,0.45,0.5,0.55,0.60]
path = 'Data/BPI2012'
res = dict()
variable=['Activity','Variant', 'Variant index','concept:name', 'impact', 'lifecycle:transition', 
#          
          'organization country', 'product']
for v in MissingRate:
    
    value = []
    for i in range(30):
        
        log = pd.read_csv(path + '/TrainLog.CSV')
        log = dataframe_utils.convert_timestamp_columns_in_df(log)
        log = log.sort_values('Complete Timestamp')
        log = log_converter.apply(log)
        net, im, fm = inductive_miner.apply(log)
        
        test_log = pd.read_csv(path + '/TestLog.CSV')
        #test_log = dataframe_utils.convert_timestamp_columns_in_df(test_log)
        #test_log = test_log.sort_values('EDT')
        #test_log = log_converter.apply(test_log)
        
        
        x = meg.MissingEventGenerate(path, test_log, variable, v)
        missing_log = x.MissingEventGenerate()
        repair_log = missing_log.dropna(subset=['concept:name'])
        repair_log.to_csv(path+'/SDLog.csv')
        
        repair_log = pd.read_csv(path + '/SDLog.CSV')
        repair_log = dataframe_utils.convert_timestamp_columns_in_df(repair_log)
        repair_log = repair_log.sort_values('Complete Timestamp')
        repair_log = log_converter.apply(repair_log)
        
        net1, _, _ = inductive_miner.apply(test_log)
        
        fitness = replay_fitness_evaluator.apply(test_log, net, im, fm, variant=replay_fitness_evaluator.Variants.ALIGNMENT_BASED)
        precision = precision_evaluator.apply(test_log, net, im, fm, variant=precision_evaluator.Variants.ALIGN_ETCONFORMANCE)
        generalization = generalization_evaluator.apply(test_log, net, im, fm)
        simplicity = simplicity_evaluator.apply(net1)
        
        value.append([fitness['averageFitness'],precision,generalization,simplicity])
    
    res.update({v:value})
    
calculate_res = []
for v in MissingRate:
    x = res[v]
    calculate_res.append(np.mean(x, axis=0))
    
calculate_res = pd.DataFrame(calculate_res)
print(calculate_res)
calculate_res.to_csv(path + "/model_quality.csv")
