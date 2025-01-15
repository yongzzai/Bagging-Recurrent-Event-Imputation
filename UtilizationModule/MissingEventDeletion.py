# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 10:11:18 2020

@author: SIM
"""
import MissingEventDection 

class MissingEventDeletion:
    
    def __init__(self, FilePath,EventLog, Key, Variable, TimeVariable, TimeFormat, 
                 
                 DeletionMethod, Cutoff):
        
        self.FilePath = FilePath
        
        self.EventLog = EventLog
        
        self.Key = Key
        
        self.Variable = Variable
        
        self.TimeVariable = TimeVariable
        
        self.TimeFormat = TimeFormat
        
        self.DeletionMethod = DeletionMethod
        
        self.Cutoff = Cutoff
                
    def MissingEventDeletion(self):
        
        Detection = MissingEventDection.MissingEventDetection(self.FilePath,self.EventLog, 
                                                              
                                    self.Key,self.Variable,self.TimeVariable,self.TimeFormat)
        
        DectionMissingbyTrace, DectionMissingbyVariable = Detection.MissingEventDetection()
        
        if(self.DeletionMethod == 'CaseBase'):
            
            DeletionCase = DectionMissingbyTrace.keys()
            
            for i in range(len(DeletionCase)):
                
               self.EventLogWithDeletMissing = 
               
               self.EventLog.loc[self.EventLog[Key]!=DeletionCase[i],:]
               
        elif(self.DeletionMethod == 'VariableBase'):
            
            VariableList = list()
            
            for i in range(len(self.DectionMissingbyVariable.keys())):
                
                if(sum(self.EventLog[self.Variable[i]].isnull())<(
                        
                        len(self.DectionMissingbyVariable[self.Variable[i])*self.Cutoff)):
                    
                    VariableList.append(Variable[i])
                    
            self.EventLogWithDeletMissing = self.EventLog[VariableList]
            
        return self.EventLogWithDeletMissing
    
    
    
        
        
        
        