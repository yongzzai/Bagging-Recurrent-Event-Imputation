import numpy as np
import pandas as pd

class ModelGenerator:
    
    def __init__(self, TrainLog, Key, Variable):
        
        self.TrainLog = TrainLog
        
        self.Key = Key
        
        self.Variable = Variable
        
        self.NumOfVariable = len(Variable)
        
        self.VariableList = {'init':0}

        for i in range(self.NumOfVariable):
    
            x0 = self.TrainLog[self.Variable[i]]
            
            x0 = x0.sort_values().drop_duplicates().reset_index(drop=True)
            
            x0 = list(x0)
    
            self.VariableList.update({self.Variable[i]:x0})
            
        self.VariableList.pop('init')
        
        self.TrainLog0 = list()

        self.KeyList = self.TrainLog[self.Key].tolist()
        
        self.KeyList = sorted(set(self.KeyList))
        
        for i in range(self.NumOfVariable):
            
            if(type(self.TrainLog[Variable[i]][0]) == np.int64):
               
               for n in range(len(self.TrainLog)):
                   
                   self.TrainLog[Variable[i]][n] = int(self.TrainLog[Variable[i]][n])
    
        for i in range(len(self.KeyList)):
            
            x = self.TrainLog.loc[self.TrainLog[self.Key]==self.KeyList[i],:]
            
            x = x.reset_index(drop = True)
            
            self.TrainLog0.append(x)
            
    def RelationModel(self):
        
        print('=================================================================')
        print('Start generating horizontal relationship models')
        
        self.HorizontalRelation = {'init':0}

        for NumOfVariable1 in range(self.NumOfVariable):
    
            print(str(round(NumOfVariable1/self.NumOfVariable*100,3)) + '% Completed')
            
            HorizontalRelation0 = {'init':0}
    
            for NumOfVariable2 in range(self.NumOfVariable):
        
                x = self.TrainLog[[self.Variable[NumOfVariable1], self.Variable[NumOfVariable2]]]
        
                if(x.columns[0]!=x.columns[1]):
            
                    x = x.sort_values([self.Variable[NumOfVariable1]])
           
                    x = x.drop_duplicates()

                    x = x.reset_index(drop = True)
            
                    HorizontalRelation1 = list()
            
                    for NumOfVariableList in range(len(self.VariableList[self.Variable[NumOfVariable1]])):
                
                        x0 = x.loc[x[self.Variable[NumOfVariable1]]==self.VariableList[self.Variable[NumOfVariable1]][NumOfVariableList]]
                
                        x0 = x0.reset_index(drop = True)
                
                        HorizontalRelation1.append(x0)
                
                    HorizontalRelation2 = {'init':0}
            
                    for NumOfHorizontalRelation1 in range(len(HorizontalRelation1)):
            
                        HorizontalRelation3 = {'init':0}
            
                        for i in range(len(HorizontalRelation1[NumOfHorizontalRelation1])):
                        
                            HorizontalRelation3.update({HorizontalRelation1[NumOfHorizontalRelation1].index[i]:
                                
                                HorizontalRelation1[NumOfHorizontalRelation1][HorizontalRelation1[NumOfHorizontalRelation1].columns[1]][i]})
            
                        HorizontalRelation3.pop('init')
                            
                        HorizontalRelation2.update({HorizontalRelation1[NumOfHorizontalRelation1][HorizontalRelation1[NumOfHorizontalRelation1].columns[0]][i]:HorizontalRelation3})
                        
                    HorizontalRelation2.pop('init')
            
                    HorizontalRelation0.update({self.Variable[NumOfVariable2]:HorizontalRelation2})   
        
                else:
                
                    pass
                
            HorizontalRelation0.pop('init')
                
            self.HorizontalRelation.update({self.Variable[NumOfVariable1]:HorizontalRelation0})
    
        self.HorizontalRelation.pop('init')
        
        print('100.0% Completed')
        print('Completed generating horizontal relationship models')
        print('=================================================================')
        print(' ')
        print('=================================================================')
        print('Start generating vertical relationship models')
        
        self.VerticalRelation = {'init':0}
        
        for NumOfVariable1 in range(self.NumOfVariable):

            print(str(round(NumOfVariable1/self.NumOfVariable*100,3)) + '% Completed')
            
            VerticalRelation0 = {'init':0} 
            
            for i in range(len(self.VariableList[self.Variable[NumOfVariable1]])):
        
                VerticalRelation1 = list() 
                
                for NumofKey in range(len(self.TrainLog0)):
                    
                    x = self.TrainLog0[NumofKey][self.Variable[NumOfVariable1]]                
                
                    if(sum(x == self.VariableList[self.Variable[NumOfVariable1]][i])>0):
                        
                        VerticalRelation1.append(x)
                        
                VerticalRelation2 = {'init':0}
                        
                for j in range(len(VerticalRelation1)):
                        
                    x = VerticalRelation1[j]
                        
                    VerticalRelation3 = {'init':0}
                         
                    for m in range(len(x)):
                            
                        VerticalRelation3.update({x.index[m]:x[x.index[m]]})
                        
                    VerticalRelation3.pop('init')
                    
                    VerticalRelation2.update({j:VerticalRelation3})
                    
                VerticalRelation2.pop('init')
                
                VerticalRelation0.update({self.VariableList[self.Variable[NumOfVariable1]][i]:VerticalRelation2})
                
            VerticalRelation0.pop('init')
            
            self.VerticalRelation.update({self.Variable[NumOfVariable1]:VerticalRelation0})
            
        self.VerticalRelation.pop('init')
        
        print('100.0% Completed')
        print('Complted generating vertical relationship models')
        print('=================================================================')
        
        self.RelationModel = {'Horizontal': self.HorizontalRelation, 'Vertical': self.VerticalRelation}
        
        return self.RelationModel
    
    def ImputeModel(self):
        
        print(' ')
        print('=================================================================')
        print('Start generating horizontal imputation models')
        
        x = self.RelationModel['Horizontal']

        self.HorizontalImpute = {'init':0}

        for NumOfVariable1 in range(self.NumOfVariable):
        
            print(str(round(NumOfVariable1/self.NumOfVariable*100,3)) + '% Completed')
            
            HorizontalImpute0 = {'init':0}
            
            for NumOfVariable2 in range(self.NumOfVariable):
            
                if(self.Variable[NumOfVariable1]!=self.Variable[NumOfVariable2]):
                
                    HorizontalImpute2 = {'init':0}
        
                    for i in range(len(self.VariableList[self.Variable[NumOfVariable1]])):
                    
                        if(len(list(x[self.Variable[NumOfVariable1]][self.Variable[NumOfVariable2]][self.VariableList[self.Variable[NumOfVariable1]][i]]))==1):
                               
                            HorizontalImpute3 = {'Method': 'SimpleImpute',
                                                 
                                                 'Value':list(x[self.Variable[NumOfVariable1]][self.Variable[NumOfVariable2]][self.VariableList[self.Variable[NumOfVariable1]][i]].values())}
        
                        else:
                            
                            HorizontalImpute3 = {'Method':'MultipleImpute',
                                                 
                                                 'Value':list(x[self.Variable[NumOfVariable1]][self.Variable[NumOfVariable2]][self.VariableList[self.Variable[NumOfVariable1]][i]].values())}
                            
                        HorizontalImpute2.update({self.VariableList[self.Variable[NumOfVariable1]][i]:HorizontalImpute3})
                        
                    HorizontalImpute2.pop('init')
                    
                    HorizontalImpute0.update({self.Variable[NumOfVariable2]:HorizontalImpute2})
                      
            HorizontalImpute0.pop('init')
                
            self.HorizontalImpute.update({self.Variable[NumOfVariable1]:HorizontalImpute0})
            
        self.HorizontalImpute.pop('init')

        print('100.0% Completed')        
        print('Complted generating horizontal imputation models')
        print('=================================================================')
        print(' ')
        print('=================================================================')
        print('Start generating vertical imputation models')
        
        x = self.RelationModel['Vertical']
        
        self.VerticalImpute = {'init':0}

        for NumOfVariable1 in range(self.NumOfVariable):
                
            print(str(round(NumOfVariable1/self.NumOfVariable*100,3)) + '% Completed')
            
            VerticalImpute0 = {'init':0}
            
            for i in range(len(self.VariableList[self.Variable[NumOfVariable1]])):
        
                x0 = x[self.Variable[NumOfVariable1]][self.VariableList[self.Variable[NumOfVariable1]][i]]
                
                iteration = 0
                
                y = np.zeros((1000,1))
                
                while(iteration!=1000):
                
                    sample = np.random.randint(0,len(list(x0.keys())),1)
                        
                    if(sum(np.array(list(x0[sample[0]].values()))==self.VariableList[self.Variable[NumOfVariable1]][i])/len(list(x0[sample[0]]))==1):
                    
                        y[iteration] = 1
                        
                    iteration += 1
                
                if((sum(y)>1000*0.9)[0]):
                
                    VerticalImpute1 = {'Method':'SimpleImpute'}
                
                else:
                    
                    VerticalImpute1 = {'Method':'MultipleImpute'}
            
                VerticalImpute0.update({self.VariableList[self.Variable[NumOfVariable1]][i]:VerticalImpute1})
                            
            VerticalImpute0.pop('init')
            
            self.VerticalImpute.update({self.Variable[NumOfVariable1]:VerticalImpute0})
                    
        self.VerticalImpute.pop('init')
        
        print('100.0% Completed')
        print('Complted generating vertical imputation models')
        print('=================================================================')
        print(' ')
        print('=================================================================')
        print('Start generating event chain imputation models')
        
        self.EventChainImpute = {'init':0}

        for NumOfVariable1 in range(self.NumOfVariable):
        
            print(str(round(NumOfVariable1/self.NumOfVariable*100,3)) + '% Completed')
            
            EventChain = pd.DataFrame(np.zeros((len(self.TrainLog),3)), columns = ['Prior','Current','Postorior'])
                
            for LenOfTrain in range(1,(len(self.TrainLog)-1)):
                    
                if(self.TrainLog[self.Key][LenOfTrain]!=self.TrainLog[self.Key][LenOfTrain-1])&(self.TrainLog[self.Key][LenOfTrain]==self.TrainLog[self.Key][LenOfTrain+1]):
                
                    EventChain['Prior'][LenOfTrain] = 'Start'
                
                    EventChain['Current'][LenOfTrain] = self.TrainLog[self.Variable[NumOfVariable1]][LenOfTrain]
                
                    EventChain['Postorior'][LenOfTrain] = self.TrainLog[self.Variable[NumOfVariable1]][LenOfTrain+1]
                
                elif(self.TrainLog[self.Key][LenOfTrain]==self.TrainLog[self.Key][LenOfTrain-1])&(self.TrainLog[self.Key][LenOfTrain]==self.TrainLog[self.Key][LenOfTrain+1]):
                
                    EventChain['Prior'][LenOfTrain] = self.TrainLog[self.Variable[NumOfVariable1]][LenOfTrain-1]
                
                    EventChain['Current'][LenOfTrain] = self.TrainLog[self.Variable[NumOfVariable1]][LenOfTrain]
                
                    EventChain['Postorior'][LenOfTrain] = self.TrainLog[self.Variable[NumOfVariable1]][LenOfTrain+1]
                    
                elif(self.TrainLog[self.Key][LenOfTrain]==self.TrainLog[self.Key][LenOfTrain-1])&(self.TrainLog[self.Key][LenOfTrain]!=self.TrainLog[self.Key][LenOfTrain+1]):
                    
                    EventChain['Prior'][LenOfTrain] = self.TrainLog[self.Variable[NumOfVariable1]][LenOfTrain-1]
                
                    EventChain['Current'][LenOfTrain] = self.TrainLog[self.Variable[NumOfVariable1]][LenOfTrain]
                
                    EventChain['Postorior'][LenOfTrain] = 'End'
                    
            EventChain = EventChain.loc[EventChain['Prior']!=0,:]
        
            EventChain = EventChain.reset_index(drop = True)
            
            TargetEventChain0 = list()
            
            for i in range(len(EventChain)):
                
                x = str(EventChain['Prior'][i]) + ">" + str(EventChain['Current'][i]) + ">" + str(EventChain['Postorior'][i])
                  
                TargetEventChain0.append(x)
                
            TargetEventChain = {'init':0}
            
            for i in range(len(TargetEventChain0)):
                
                TargetEventChain.update({i:TargetEventChain0[i]})
                
            TargetEventChain.pop('init')
                
            self.EventChainImpute.update({self.Variable[NumOfVariable1]:TargetEventChain})
                
        self.EventChainImpute.pop('init')
        
        print('100.0% Completed')
        print('Completed generating event chain imputation models')
        print('=================================================================')
        
        self.ImputeModel = {'Horizontal': self.HorizontalImpute, 'Vertical': self.VerticalImpute, 'EventChain': self.EventChainImpute}
        
        return self.ImputeModel
        
        
