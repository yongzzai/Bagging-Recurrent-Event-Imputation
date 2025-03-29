
import numpy as np
import pandas as pd

class LogSplit:
    
    def __init__(self, FilePath, FileName, SplitRatio, Key, TimeVariable, TimeFormat):
        
        self.FilePath = FilePath
        
        self.FileName = FileName
        
        self.EventLog = pd.read_csv(self.FilePath + '/' + self.FileName)
        
        self.SplitRatio = SplitRatio
        
        self.Key = Key
        
        self.TimeVariable = TimeVariable
        
        self.TimeFormat = TimeFormat
                
    def LogSplit(self):
        
        for i in range(len(self.TimeVariable)):
    
            self.EventLog[self.TimeVariable[i]] = pd.to_datetime(self.EventLog[self.TimeVariable[i]],
                         
                                                                 format=self.TimeFormat)    

        self.EventLog = self.EventLog.sort_values([self.Key,self.TimeVariable[0]])
        
        Case = self.EventLog[self.Key].drop_duplicates().reset_index(drop = True)

        TotalCase = pd.DataFrame(np.zeros((len(Case),2)),columns = ['Case','Type1'])
        
        TotalCase['Case'] = Case
        
        TotalCase['Type1'] = 'Total'
        
        TrainSample = Case[np.random.randint(0,len(Case),round(len(Case)*self.SplitRatio))].reset_index(drop = True)
        
        TrainCase = pd.DataFrame(np.zeros((len(TrainSample),2)),columns = ['Case','Type2'])
        
        TrainCase['Case'] = TrainSample
        
        TrainCase['Type2'] = 'Train'
        
        Case = pd.merge(TotalCase,TrainCase,how = 'outer')
            
        TrainCase = Case.loc[Case['Type2']=='Train']['Case'].values
        
        TestCase = Case.loc[Case['Type2'].isnull(),:]['Case'].values
        
        self.TrainLog = self.EventLog.loc[self.EventLog[self.Key]==TrainCase[0],:]     
        
        for i in range(1,len(TrainCase)):
            
            x = self.EventLog.loc[self.EventLog[self.Key]==TrainCase[i],:]
            
            self.TrainLog = self.TrainLog.append(x)
            
            self.TrainLog = self.TrainLog.reset_index(drop = True)
            
        self.TestLog = self.EventLog.loc[self.EventLog[self.Key]==TestCase[0],:]     
        
        for i in range(1,len(TestCase)):
            
            x = self.EventLog.loc[self.EventLog[self.Key]==TestCase[i],:]
            
            self.TestLog = self.TestLog.append(x)
            
            self.TestLog = self.TestLog.reset_index(drop = True)
        
        self.TrainLog.to_csv(self.FilePath + '/TrainLog.csv',index = False)
        
        self.TestLog.to_csv(self.FilePath + '/TestLog.csv', index = False)
        
        return self.TrainLog, self.TestLog
        
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
            
        self.MissingLog.to_csv(self.FilePath + '/MissingLog/TestLogWithMissing' + str(int(self.MissingRate*100)) + '%.csv')
            
        return self.MissingLog
#    
#class LogForConformanceChecking:
#    
#    def __init__(self, FilePath, RepairLog, MissingRate):
#        
#        self.FilePath = FilePath
#        
#        self.RepairLog = RepairLog
#        
#        self.MissingRate = MissingRate
#        
#    def LogForConformanceChecking(self):
        
        
    
        
        
        
        
