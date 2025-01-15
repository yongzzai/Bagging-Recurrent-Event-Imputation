# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 10:11:18 2020

@author: SIM
"""
import numpy as np

class MissingEventGenerate:
    
    def __init__(self, FilePath, EventLog, Variable, MissingRate):
        
        self.FilePath = FilePath
        
        self.EventLog = EventLog
        
        self.MissingRate = MissingRate
        
        self.Variable = Variable
        
    def MissingEventGenerate(self):
        
        self.MissingLog = self.EventLog.copy()
        
        MissingNumber = list()
        
        # Missing Ratio is 0.1
        
        for i in range(len(self.Variable)):
        
            x = np.random.randint(0,len(self.MissingLog),
                                  
                                  round(len(self.MissingLog)*self.MissingRate))
    
            MissingNumber.append(x)
            
        for i in range(len(self.Variable)):   
        
            self.MissingLog.loc[MissingNumber[i],self.Variable[i]] = np.nan
            
        self.MissingLog.to_csv(self.FilePath + '/MissingLog/TestLogWith' + str(int(self.MissingRate*100)) + 'Missing%.csv')
            
        return self.MissingLog

        
    
        
        
        
        