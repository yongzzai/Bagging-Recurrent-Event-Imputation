# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 10:11:18 2020

@author: SIM
"""
import pandas as pd
import numpy as np

class MissingEventDetection:
    
    def __init__(self, FilePath, EventLog, Key, Variable, TimeVariable, TimeFormat):
        
        self.FilePath = FilePath
        
        self.EventLog = EventLog
        
        self.Key = Key
        
        self.TimeVariable = TimeVariable
        
        self.TimeFormat = TimeFormat
        
    def MissingEventDetection(self):
        
        for i in range(len(self.TimeVariable)):
    
            self.EventLog[self.TimeVariable[i]] = pd.to_datetime(self.EventLog[self.TimeVariable[i]],
                         
                                                                 format=self.TimeFormat)    

        self.EventLog = self.EventLog.sort_values([self.Key,self.TimeVariable[0]])

#        self.AnalysisLog = self.EventLog.copy()
        
        self.LogbyVariable = dict()
        
        self.Case = self.EventLog[self.Key].drop_duplicates().reset_index(drop = True)
        
        self.LogbyTrace = dict()
        
        for i in range(len(self.Case)):
            
            self.LogbyTrace.update({self.Case[i]:self.EventLog.loc[self.EventLog[self.Key]==self.Case[i],:]})
            
        self.DetectMissingbyTrace = dict()
        
        for i in range(len(self.Case)):
            
            if(sum(self.LogbyTrace[self.Case[i]].isnull())>0):
                
                self.DetectMissingbyTrace.update({self.Case[i]:sum(self.LogbyTrace[self.Case[i]].isnull())})
                
        self.LogbyVariable = dict()
        
        for i in range(len(self.Variable)):
            
            self.LogbyVariable.update({self.Variable[i]:self.EventLog[self.Variable[i]]})
            
        self.DetectMissingbyVariable = dict()
        
        for i in range(len(self.Variable)):
            
            if(sum(self.LogbyVariable[self.Variable[i]].isnull())>0):
                
                self.DetectMissingbyVariable.update({self.Variable[i]:sum(
                        
                        self.LogbyVariable[self.Variable[i]].isnull())})
                
        return self.DetectMissingbyTrace, self.DetectMissingbyVariable
        
        
        
        