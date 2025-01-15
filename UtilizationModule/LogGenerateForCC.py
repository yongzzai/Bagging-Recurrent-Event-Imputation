# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 10:11:18 2020

@author: SIM
"""

class LogGenerateForCC:
    
    def __init__(self, FilePath, MissingLog, Activity, MissingRate):
        
        self.FilePath = FilePath
        
        self.MissingLog = MissingLog
        
        self.Activity = Activity
        
        self.MissingRate = MissingRate
                
    def LogGenerateForCC(self):
        
        self.MissingLog = self.MissingLog.loc[self.MissingLog[self.Activity].notnull(),:]

        filename = self.FilePath + '/Conformance Checking Test/DeleteLogWith' + str(int(self.MissingRate*100)) + 'Missing%.csv'
    
        self.MissingLog.to_csv(filename, header=True, index = False)
    
        
    
        
        
        
        