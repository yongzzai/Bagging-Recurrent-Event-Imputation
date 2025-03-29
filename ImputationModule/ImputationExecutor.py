import numpy as np
import pandas as pd
import math as m

class ImputationExecutor:
    
    def __init__(self, EventLog, ImputationModel, Key, Variable, TimeVariable, TimeFormat, 
                 
                 BootstrapIteration, Iteration1, Iteration2, BoostrapingParameter):
        
        self.EventLog = EventLog 
        
        self.LenOfEventLog = len(self.EventLog)
        
        self.Key = Key
        
        self.KeyList =  self.EventLog[self.Key].sort_values().drop_duplicates().reset_index(drop=True)
        
        self.Variable = Variable
        
        self.NumOfVariable = len(self.Variable)
        
        self.VariableList = {'init':0}

        for NumOfVariable1 in range(self.NumOfVariable):
    
            x0 = list(self.EventLog[self.Variable[NumOfVariable1]].value_counts().index.values)
    
            self.VariableList.update({self.Variable[NumOfVariable1]:x0})
            
        self.VariableList.pop('init')
        
        self.TimeVariable = TimeVariable
        
        self.TimeFormat = TimeFormat
        
        for i in range(len(TimeVariable)):
            
            self.EventLog[TimeVariable[i]] =  pd.to_datetime(self.EventLog[TimeVariable[i]],
                         
                         format= self.TimeFormat)
            
        self.SortKey = list()
        
        self.SortKey.append(Key)
        
        self.SortKey.extend(TimeVariable)
        
        self.EventLog = self.EventLog.sort_values(self.SortKey)
        
        self.EventLog = self.EventLog.reset_index(drop = True)
        
        self.Iteration1 = Iteration1
        
        self.Iteration2 = Iteration2
        
        self.BootstrapIteration = BootstrapIteration
        
        self.BoostrapingParameter = BoostrapingParameter
        
        self.ImputationModel = ImputationModel
        
        self.InitRepairLog = list()

        self.Result = EventLog

        for i in range(len(self.KeyList)):
            
            x = self.EventLog.loc[self.EventLog[self.Key]==self.KeyList[i],:]
            
            x = x.reset_index(drop = True)
            
            self.InitRepairLog.append(x)
        
        self.RepairLog0 = pd.DataFrame()
            
    def RecurrentEventImputation(self):
        
            self.FinalRepairLog = list()
            
            self.RepairLog = self.InitRepairLog.copy()
            
            iter0 = 0
            
            while(iter0 < self.BootstrapIteration):
                    
                iter1 = 0
            
                while(iter1 < self.Iteration1):
                    
                    if iter1!=0:
                        
                        self.RepairLog = self.ResultLog.copy()
                        
                    else:
                        
                        self.RepairLog = self.RepairLog.copy()
                                
                    print('=================================================================')
                    print('Start ' + str(iter1+1) + '-th Simple Vertical Event Imputation')
                    
                    self.RepairLog0 = pd.DataFrame()
                    
                    for NumOfCase in range(len(self.KeyList)):
                
                        for NumOfVariable1 in range(self.NumOfVariable):
                    
                            if(sum(self.RepairLog[NumOfCase][self.Variable[NumOfVariable1]].isnull())>0):
                    
                                x = list(self.ImputationModel['Imputation']['Vertical'][self.Variable[NumOfVariable1]])
                                    
                                VerticalSimpleTest = np.zeros(len(x))
                                    
                                for n in range(len(VerticalSimpleTest)):
                                        
                                    if(list(self.ImputationModel['Imputation']['Vertical'][self.Variable[NumOfVariable1]][x[n]].values())[0]=='SimpleImpute'):
                                            
                                        VerticalSimpleTest[n] = 1
                                            
                                if(sum(VerticalSimpleTest)>(len(VerticalSimpleTest)*0.9)):
                             
                                    RepairValue = self.RepairLog[NumOfCase][self.Variable[NumOfVariable1]].loc[self.RepairLog[NumOfCase][self.Variable[NumOfVariable1]].notnull()]
                                         
                                    if(len(RepairValue)!=0):
                                    
                                        RepairValue = RepairValue.drop_duplicates().values[0]                    
                                         
                                        self.RepairLog[NumOfCase][self.Variable[NumOfVariable1]] = RepairValue
                                        
                                    else:
                                        
                                        pass
                                         
                    print('Completed ' + str(iter1+1) + '-th Simple Vertical Event Imputation')
                    print('=================================================================')
                    print(' ')
                
                    # Simple Horizontal Event Imputation 
                                    
                    #[Reference Variable][Repair Variable][Reference Value].Value = Repair Value
                
                    print('=================================================================')
                    print('Start ' + str(iter1+1) + '-th Simple Horizontal Event Imputation')
                    
                    for NumOfCase in range(len(self.KeyList)):
                        
                        for NumOfVariable2 in range(self.NumOfVariable):
                    
                            if(sum(self.RepairLog[NumOfCase][self.Variable[NumOfVariable2]].isnull().values)>0):
                                    
                                for NumOfVariable1 in range(self.NumOfVariable):
                            
                                    if(self.Variable[NumOfVariable1]!=self.Variable[NumOfVariable2]):
                        
                                        for LenOfTrace in range(len(self.RepairLog[NumOfCase])):
                                
                                            if((self.RepairLog[NumOfCase][self.Variable[NumOfVariable2]].isnull()[LenOfTrace])&
                                            
                                               (self.RepairLog[NumOfCase][self.Variable[NumOfVariable1]].notnull()[LenOfTrace])):
                                                
                                                ValueList = list(self.ImputationModel['Imputation']['Horizontal'][self.Variable[NumOfVariable1]][self.Variable[NumOfVariable2]].keys())
                                
                                                for j in range(len(ValueList)):
                                        
                                                    if((self.RepairLog[NumOfCase][self.Variable[NumOfVariable1]][LenOfTrace]==
                                                       
                                                       list(self.ImputationModel['Imputation']['Horizontal'][self.Variable[NumOfVariable1]][self.Variable[NumOfVariable2]].keys())[j])):
                                                
                                                        if(self.ImputationModel['Imputation']['Horizontal'][self.Variable[NumOfVariable1]][self.Variable[NumOfVariable2]][ValueList[j]]['Method']=='SimpleImpute'):
            
                                                            RepairValue = self.ImputationModel['Imputation']['Horizontal'][self.Variable[NumOfVariable1]][self.Variable[NumOfVariable2]][ValueList[j]]['Value'][0]
                                                    
                                                            self.RepairLog[NumOfCase][self.Variable[NumOfVariable2]][LenOfTrace] = RepairValue
        
                                                        else:
        
                                                            pass
                                                            
                    print('Completed ' + str(iter1+1) + '-th Simple Horizontal Event Imputation')
                    print('=================================================================')
                    print(' ')
                    
                    #Multiple Event Chain Imputation
                    
                    self.RepairLog0 = self.RepairLog[0]
                    
                    for NumOfCase in range(1,len(self.KeyList)):
                        
                        x = self.RepairLog[NumOfCase]
                        
                        self.RepairLog0 = pd.concat([self.RepairLog0, x], ignore_index=True)
                
                        self.RepairLog0 = self.RepairLog0.reset_index(drop = True)
                    
                    self.KLD = {'init':100}

                    for NumOfVariable0 in range(self.NumOfVariable):
                        
                        self.KLD.update({self.Variable[NumOfVariable0]:100})
                        
                    self.KLD.pop('init')
                    
                    print('=================================================================')
                    print('Start ' + str(iter1+1) + '-th Multiple Event Imputation')
                    
                    iter2 = 0
                                
                    while(iter2!=self.Iteration2):
                    
                        self.RepairLog1 = self.RepairLog.copy()
                    
                        for NumOfVariable1 in range(self.NumOfVariable):
                    
                            sample = np.random.randint(0,len(self.ImputationModel['Imputation']['EventChain'][self.Variable[NumOfVariable1]]),
                                                               
                                                       round(len(self.ImputationModel['Imputation']['EventChain'][self.Variable[NumOfVariable1]])*self.BoostrapingParameter))
                    
                            Target = pd.DataFrame(self.ImputationModel['Imputation']['EventChain'][self.Variable[NumOfVariable1]].values(), columns = ['EventChain'])
                            
                            Target = Target['EventChain'][sample]
                            
                            Target = Target.reset_index(drop = True)
                            
                            TargetValue = Target.value_counts()/len(Target)
                            
                            TargetValue = TargetValue.reset_index()
                        
                            for NumOfCase in range(len(self.KeyList)):
                            
                                if(sum(self.RepairLog1[NumOfCase][self.Variable[NumOfVariable1]].isnull().values)>0)&(sum(self.RepairLog1[NumOfCase][self.Variable[NumOfVariable1]].notnull().values)>0):
                                    
                                    ValueList = self.RepairLog1[NumOfCase][self.Variable[NumOfVariable1]].loc[self.RepairLog1[NumOfCase][self.Variable[NumOfVariable1]].notnull()].values
                                    
                                    if(len(ValueList)>1):
                                        
                                        SampleList = list(self.ImputationModel['Relation']['Vertical'][self.Variable[NumOfVariable1]][ValueList[0]].values())
                                        
                                        for i in range(1,len(ValueList)):
                                            
                                            try:
                                            
                                                x = list(self.ImputationModel['Relation']['Vertical'][self.Variable[NumOfVariable1]][ValueList[i]].values())
                                                
                                                SampleList.extend(x)
                                                
                                            except:
                                                
                                                pass
                                                    
                                    else:
                                        
                                        SampleList = list(self.ImputationModel['Relation']['Vertical'][self.Variable[NumOfVariable1]][ValueList[0]].values())
                                        
                                    SampleNumber = np.random.randint(0,len(SampleList),1)[0]
                                            
                                    Sample = SampleList[SampleNumber]
                                    
                                    if(len(Sample)==len(self.RepairLog1[NumOfCase])):
                                    
                                        for n in range(len(Sample)):
                                            
                                            if(self.RepairLog1[NumOfCase][self.Variable[NumOfVariable1]].isnull()[n]):
                                                            
                                                self.RepairLog1[NumOfCase][self.Variable[NumOfVariable1]][n] = Sample[n]
                                                
                        self.RepairLog2 = self.RepairLog1[0]
                
                        for NumOfCase in range(1,len(self.KeyList)):
            
                            x = self.RepairLog1[NumOfCase]
            
                            self.RepairLog2 = pd.concat([self.RepairLog2, x], ignore_index=True)
            
                            self.RepairLog2 = self.RepairLog2.reset_index(drop = True)                      
                                     
                        RepairEventChain0 = pd.DataFrame(np.zeros((len(self.RepairLog2),3),int), columns = ['Prior','Current','Postorior'])
                    
                        for NumOfVariable1 in range(self.NumOfVariable):
                    
                            for n in range(1,(self.LenOfEventLog)-1):
                
                                if((self.RepairLog2[self.Key][n]!=self.RepairLog2[self.Key][n-1])&
                                
                                (self.RepairLog2[self.Key][n]==self.RepairLog2[self.Key][n+1])):
                            
                                    RepairEventChain0['Prior'][n] = 'Start'
                                
                                    RepairEventChain0['Current'][n] = self.RepairLog2[self.Variable[NumOfVariable1]][n]
                                
                                    RepairEventChain0['Postorior'][n] = self.RepairLog2[self.Variable[NumOfVariable1]][n+1]
                                
                                elif((self.RepairLog2[self.Key][n]==self.RepairLog2[self.Key][n-1])&
                                     
                                     (self.RepairLog2[self.Key][n]==self.RepairLog2[self.Key][n+1])):
                                
                                    RepairEventChain0['Prior'][n] = self.RepairLog2[self.Variable[NumOfVariable1]][n-1]
                                
                                    RepairEventChain0['Current'][n] = self.RepairLog2[self.Variable[NumOfVariable1]][n]
                                
                                    RepairEventChain0['Postorior'][n] = self.RepairLog2[self.Variable[NumOfVariable1]][n+1]
                            
                                elif((self.RepairLog2[self.Key][n]==self.RepairLog2[self.Key][n-1])&
                                     
                                     (self.RepairLog2[self.Key][n]!=self.RepairLog2[self.Key][n+1])):
                                    
                                    RepairEventChain0['Prior'][n] = self.RepairLog2[self.Variable[NumOfVariable1]][n-1]
                                
                                    RepairEventChain0['Current'][n] = self.RepairLog2[self.Variable[NumOfVariable1]][n]
                        
                            RepairEventChain0 = RepairEventChain0.loc[RepairEventChain0['Prior']!=0,:]
                            
                            RepairEventChain0 = RepairEventChain0.reset_index(drop = True)
                            
                            RepairEventChain = []
                    
                            for i in range(len(RepairEventChain0)):
                                
                                x = str(RepairEventChain0['Prior'][i]) + '>' + str(RepairEventChain0['Current'][i]) + '>' + str(RepairEventChain0['Postorior'][i])
                                
                                RepairEventChain.append(x)
                        
                            RepairEventChain = pd.DataFrame(RepairEventChain, columns = ['EventChain1'])
                    
                            RepairValue = RepairEventChain['EventChain1'].value_counts()/len(RepairEventChain)                            
                        
                            RepairValue = RepairValue.reset_index()
                            
                            RepairValue.rename(columns={'index':'EventChain'}, inplace=True)

                            ValueTable = pd.merge(TargetValue, RepairValue, how='outer')
                            
                            ValueTable['count1'] = ValueTable['EventChain1'].value_counts()/len(ValueTable)

                            #print("\n\n\n\n\n\n\n========================================")

                            #print("ValueTable ValueTable ValueTable")

                            #print()

                            #print(ValueTable)

                            #print()

                            ValueTable.fillna(1e-15, inplace=True)
                            
                            NewKLD = np.zeros((len(ValueTable),4))
                            
                            for i in range(len(ValueTable)):

                                NewKLD[i,0] = ValueTable['count'][i]
                                
                                NewKLD[i,1] = m.log(float(ValueTable['count'][i]))
                                
                                NewKLD[i,2] = m.log(float(ValueTable['count1'][i]))
                            
                            for i in range(len(ValueTable)):
                                
                                NewKLD[i,3] = NewKLD[i,0] * (NewKLD[i,1] - NewKLD[i,2])
                                
                            NewKLDValue = sum(NewKLD[:,3])
                                    
                            if(self.KLD[self.Variable[NumOfVariable1]]>NewKLDValue):
                                
                                self.KLD[self.Variable[NumOfVariable1]] = NewKLDValue
                                
                                self.Result = self.RepairLog2
                                
                            print(str(iter1+1) +'-th Mutiple Event Imputation: ' + str(iter2+1) + ', Variable: ' + self.Variable[NumOfVariable1] + 
                                  
                                  ', Cost: ', self.KLD[self.Variable[NumOfVariable1]])   
                    
                        iter2 += 1
            
                    print('Completed ' + str(iter1+1) + '-th Multiple Event Imputation')
                    print('=================================================================')
                    print(' ')

                    self.Result = self.Result.sort_values(self.SortKey)

                    self.Result = self.Result.reset_index(drop = True)

                    self.ResultLog = []

                    for NumOfCase in range(len(self.KeyList)):
            
                        x = self.Result.loc[self.Result[self.Key]==self.KeyList[NumOfCase],:]
            
                        x = x.reset_index(drop = True)
            
                        self.ResultLog.append(x)
                
                    iter1 += 1
            
                self.FinalRepairLog.append(self.Result)
            
                iter0 += 1
        
            return self.FinalRepairLog                                                                            
            
