#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Para medir el tiempo del código. 
import time
inicio = time.time()


# #### Download Realizados

# #### Evaluación

# In[2]:


#IMPORTACIÓN DE LIBERÍAS
import pandas as pd

import datetime as dt
from datetime import date, timedelta

import pymysql
import os

#Set display
pd.set_option('display.max_rows', 500) 
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# In[3]:


#DECLARACIÓN DEL PARÁMETRO: día
#Objetivo: establecer el día que evaluaremos

hoy= pd.Timestamp('today')
ayer_fechacompleta= (pd.Timestamp('today')- timedelta(days=1)).strftime("%Y%m%d")
dia_ecd= (pd.Timestamp('today')  - timedelta(days=1)).strftime("%d")
dia_ec= (pd.Timestamp('today')  - timedelta(days=1)).strftime("%Y%m%d")
dia_ecr= (pd.Timestamp('today')  - timedelta(days=1)).strftime("%d%m%Y")

dos_dias_antes_fechacompleta = (pd.Timestamp('today')- timedelta(days=2)).strftime("%Y%m%d")
dia_compd=(pd.Timestamp('today')- timedelta(days=2)).strftime("%d") #día
dia_comp=(pd.Timestamp('today')- timedelta(days=2)).strftime("%Y%m%d") #archivo
dia_compr=(pd.Timestamp('today')- timedelta(days=2)).strftime("%d%m%Y") #reversa
    
print("Dia_ejecucion:",hoy,'\n')
print("ayer:",dia_ecd)
print("dos días antes(dia_comp):",dia_compd)

print("dos días antes(dia_comp)fecha completa:",dia_comp)
print("ayer fecha completa:",dia_ec)


# In[6]:


#SFTP ALTAN
import pymysql 
import pandas as pd
import numpy as np

conn = pymysql.connect(host='rdso.wiwi.services', port=3306, user='martin', passwd='nomepierdas')
#sql2 = """select * from altan_seq.sftp where fromfile = 'EstadoConsumo_168_202301{}.csv'""".format(dia_ec)
sql2 = """select * from altan_seq.sftp where fromfile = 'EstadoConsumo_168_{}.csv'""".format(dia_ec)
estados = pd.read_sql(sql2,con=conn)

#estados.drop(['idsftp', 'fromfile'], axis=1, inplace=True)

#estados.drop(['idsftp'], axis=1, inplace=True)

#declaración colnames
list_names= ['idsftp'
            ,'Fecha Transaccion'
            ,'Fecha Medicion'
            ,'Cliente'
            ,'Fecha Inicio PF'
            ,'Fecha Fin PF'
            ,'MSISDN'
            ,'Offer ID'
            ,'Offer Name'
            ,'FreeUnit ID'
            ,'Unidad Medida'
            ,'Fecha Inicial Activacion UF'
            ,'Fecha Inicio Producto Primario'
            ,'Fecha Fin Producto Primario'
            ,'Estado Tarificado'
            ,'Consumo Medido RGU Diario'
            ,'Dias RGU Cambio Domic'
            ,'Dias Edo Activo RGU y CI'
            ,'Dias Edo Baja RGU y CI'
            ,'Dias Edo Suspendido RGU y CI'
            ,'Consumo Medido Acum RGU-UF y CI Edo Activo 10'
            ,'Consumo Medido Acumulado RGU-UF y CI Edo Baja 20'
            ,'Consumo Medido Acumulado RGU-UF y CI Edo Suspendido 30'
            ,'Consumo Medido Acumulado RGU Cambio Domic'
            ,'Consumo Excedente de su Cuota de Datos'
            ,'Unit_used_rr'
            ,'Unit_used_cumul_rr'
            ,'Fromfile']

#list_names en estados
estados.columns=list_names

#CONVERSIÓN DE 'Consumo Medido ...' de bits a GB.
estados['GB'] = round((estados['Consumo Medido Acum RGU-UF y CI Edo Activo 10']/1073741824),4)

fecha = estados['Fecha Medicion'][0]
evaluacion = estados[(estados['Fecha Transaccion'] == fecha)] 

evaluacion.drop(['idsftp', 'Fromfile'], axis=1, inplace=True)

print("Fecha medición:",fecha)
print("Routers:",evaluacion.MSISDN.nunique()) #MSISDN =router
print("Registros totales de los Routers:",evaluacion.shape[0]) 


# In[7]:


####cambié a set() y luego .add

#IDENTIFICACIÓN DE 5 CASOS: 
#OBJETIVO DE PROCESO: saber cuántas bolsas y cuántos GB tenemos.

#NOTA: cada dispositivo puede tener una o más bolsas, 

#MSISDN = Router
dns_unicos = evaluacion.MSISDN.unique() 

#Lista para Caso A: Una bolsa y no se ha consumido nada 
#nota (para esta y listas posteriores):consumo=valor de bolsa; 

#ej. si es 0 no se ha consumido nada)
lista_A = [] 

#Lista para Caso B:Una bolsa y ya empezó a consumir porque es dif de 0.
lista_B = []

#Lista para Caso C: + de 1 bolsa & todas las bolsas que se contrataron NO se han consumido.
lista_C = []

#Lista para Caso D: + de 1 bolsa y AL MENOS una bolsa Sí se está consumiendo.
lista_D = []

#Lista para Caso E: + de 1 bolsa y todas se están ocupando
lista_E = []

for i in dns_unicos:
    subconjunto = evaluacion[evaluacion.MSISDN == i]
    num_bolsas = subconjunto['FreeUnit ID'].nunique() #FreeUnitID =ID de la bolsa contratada.
    
    if num_bolsas == 1:
        
        valor_de_bolsa = subconjunto['Consumo Medido Acum RGU-UF y CI Edo Activo 10'].values[0]
        
        if valor_de_bolsa == 0:
            lista_A.append(i)
        else:
            lista_B.append(i)
    
    elif num_bolsas > 1:
        
        valor_de_bolsa = subconjunto.GB.unique().tolist() 
        
        if (len(valor_de_bolsa) == 1) & (0 in valor_de_bolsa):
            lista_C.append(i)
        elif (len(valor_de_bolsa) != 1) & (0 in valor_de_bolsa):
            lista_D.append(i)
        elif(0 not in valor_de_bolsa):
            lista_E.append(i)

print('Preproceso')           
print(fecha)


#CASO A: tiene una bolsa y el consumo es 0 porque no ha consumido nada
print('Caso A, {} con 1 bolsa y consumo = 0'.format(len(lista_A))) 

#CASO B: tiene una bolsa y ya empezó a consumir porque es dif de 0
print('Caso B, {} con 1 bolsa y consumo dif 0 '.format(len(lista_B)))

#CASO C: dispositivo con más de una bolsa contratada pero todas las bolsas que se contrataron no se han empezado a usar
print('Caso C, {} con más de 1 bolsa y consumos = 0 '.format(len(lista_C)))

#CASO D: más de una bolsa contratadas y al menos una de esas bolsas sí se está consumiendo
print('Caso D, {} con más de 1 bolsa y al menos un consumo != 0 '.format(len(lista_D)))

#CASO E: más de una bolsa y todas se están ocupando
print('Caso E, {} con más de 1 bolsa y ninguna en un consumo != 0 '.format(len(lista_E)))



# #### Filtrado por casos

# In[8]:


#CASO D
caso_a_componer = evaluacion[evaluacion.MSISDN.isin(lista_D)]
print(caso_a_componer.MSISDN.nunique())
print(caso_a_componer.shape[0])


# In[9]:


#Limpieza caso D
ajuste_caso_D = []

for i in lista_D:
    
    subconjunto1 = evaluacion[evaluacion.MSISDN == i ]
    valores_de_bolsas_D = subconjunto1['Consumo Medido Acum RGU-UF y CI Edo Activo 10'].unique().tolist() 
    consumo_diario = subconjunto1['Consumo Medido RGU Diario'].unique().tolist() 
    
    if (0 in valores_de_bolsas_D) &(len(consumo_diario)>1):
       
        dia = int(subconjunto1[subconjunto1['Consumo Medido RGU Diario'] != 0]['Dias Edo Activo RGU y CI'].values[0])
    
        subconjunto2 = subconjunto1[subconjunto1['Dias Edo Activo RGU y CI'] <= dia]
    else:
        
        subconjunto2 = subconjunto1
        
    ajuste_caso_D.append(subconjunto2)
    
parte_D = pd.concat(ajuste_caso_D)
print(parte_D.shape[0])
print(caso_a_componer.shape[0])


# In[10]:


#ASIGNACIÓN DE ETIQUETAS EN DF PRINCIAL, SEGÚN CASO

parte_A = evaluacion[evaluacion.MSISDN.isin(lista_A)]
parte_A['Caso'] = 'A'

parte_B = evaluacion[evaluacion.MSISDN.isin(lista_B)]
parte_B['Caso'] = 'B'

parte_C = evaluacion[evaluacion.MSISDN.isin(lista_C)]
parte_C['Caso'] = 'C'

parte_D['Caso'] = 'D'

parte_E = evaluacion[evaluacion.MSISDN.isin(lista_E)]
parte_E['Caso'] = 'E'


# In[11]:


df_nuevo = pd.concat([parte_A,parte_B,parte_C,parte_D,parte_E])
print(df_nuevo.shape[0])
print(df_nuevo.MSISDN.nunique())


# In[12]:


#Una vez limpio el DF, realiza misma lógica y obtenemos resumen final

lista_A = []
lista_B = []
lista_C = []
lista_D = []
lista_E = []

for i in dns_unicos:
    subconjunto = df_nuevo[df_nuevo.MSISDN == i]
    num_bolsas = subconjunto['FreeUnit ID'].nunique()
    
    if num_bolsas == 1:
        
        valor_de_bolsa = subconjunto['Consumo Medido Acum RGU-UF y CI Edo Activo 10'].values[0]
        
        if valor_de_bolsa == 0:
            lista_A.append(i)
        else:
            lista_B.append(i)
    
    elif num_bolsas > 1:
        
        valor_de_bolsa = subconjunto.GB.unique().tolist() 
        
        if (len(valor_de_bolsa) == 1) & (0 in valor_de_bolsa):
            lista_C.append(i)
        elif (len(valor_de_bolsa) != 1) & (0 in valor_de_bolsa):
            lista_D.append(i)
        elif(0 not in valor_de_bolsa):
            lista_E.append(i)

print('------Proceso')           
print(fecha)
print('Total DNS unicos {}'.format(len(dns_unicos)))
print('\n')
print('Caso A, {} con 1 bolsa y consumo = 0'.format(len(lista_A)))
print('Caso B, {} con 1 bolsa y consumo dif 0 '.format(len(lista_B)))
print('Caso C, {} con mas de 1 bolsa y consumos = 0 '.format(len(lista_C)))
print('Caso D, {} con mas de 1 bolsa y al menos un consumo != 0 '.format(len(lista_D)))
print('Caso E, {} con mas de 1 bolsa y todos los consumos != 0 '.format(len(lista_E)))


# In[13]:


#EXTRACCIÓN DE FECHA DE CONTRATACIÓN DE BOLSA, SEGÚN CASO: 

#OBJETIVO DE PROCESO: Los routers que tienen más de una bolsa, tienen más de una fecha de contratación 
#(de bolsa), por lo que manipulamos fecha. Para ello ordenamos de menor a mayor y 
#nos quedamos con la primera fecha, porque eso nos dirá desde cuándo lo contratamos.

#cambio de columna formato fecha
df_nuevo['Fecha Inicio Producto Primario'] = pd.to_datetime(df_nuevo['Fecha Inicio Producto Primario'], 
                                                            format='%d/%m/%Y')

#Asigna letra segúnc caso, en nueva columna Caso
parte_A = df_nuevo[df_nuevo.MSISDN.isin(lista_A)]
parte_A['Caso'] = 'A'

parte_B = df_nuevo[df_nuevo.MSISDN.isin(lista_B)]
parte_B['Caso'] = 'B'

parte_C = df_nuevo[df_nuevo.MSISDN.isin(lista_C)]
parte_C['Caso'] = 'C'

parte_D['Caso'] = 'D'

parte_E = df_nuevo[df_nuevo.MSISDN.isin(lista_E)]
parte_E['Caso'] = 'E'

df_nuevo = pd.concat([parte_A,parte_B,parte_C,parte_D,parte_E])

#Ordena fecha de menor a mayor y se queda con la primera. 
df_nuevo=df_nuevo.sort_values(['MSISDN','Fecha Inicio Producto Primario'], 
                              ascending=[True, False])

#Elimina duplicados de 'MSISDN' y se queda con el primero
final=df_nuevo.drop_duplicates(['MSISDN'], keep='first')
final['Fecha Transaccion'] = final['Fecha Transaccion'].dt.strftime('%d/%m/%Y')


print(final.shape[0])
print(final.MSISDN.nunique())


# In[14]:


#SELECCIÓN DE COLUMNAS QUE NOS INTERESAN (EXPLICACIÓN DE ALGUNAS COLUMNAS ABAJO)

final = final[['Fecha Transaccion','MSISDN','Offer ID','Offer Name','Consumo Medido Acum RGU-UF y CI Edo Activo 10','Dias Edo Activo RGU y CI','Fecha Inicio Producto Primario','GB','Caso']]
print(final.shape)
print(final.MSISDN.nunique())

#Al final las columnas quedaron (Explicación de columnas después de >>): 
#'Fecha Transaccion'
#'MSISDN'>> router
#'Offer ID'
#'Offer Name'
#'Consumo Medido Acum RGU-UF y CI Edo Activo 10'>> cuánto ha consumido
#'Dias Edo Activo RGU y CI'>> cuántos días lleva activo
#'Fecha Inicio Producto Primario'>> cuándo se contrató la bolsa
#'GB'>> cuántos GB lleva consumido
#'Caso'>> qué tipo de caso es


# #### Asignaciones

# In[15]:


#ASGINACIONES 

#OBJETIVO DE PROCESO: saber si los MSISDN (ROUTER) fueron asignados o no a una unidad de transporte, 
#Trae información como: tipo de ruta, rótulo de unida de transporte y DN conectados en esos rótulos 

#con base en rótulo de unidad.

#Necesitamos los siguientes archivos:
    #1. 'camiones.csv'
    #2. 'paraderos.csv'
    #3. 'DNS_MTY_IT1.xlsx'
    #4. 'Realizados-2022-12-19.csv'


#LECTURA ASGINACIONES   
#sql3 = """select * from control_radio.asignaciones_ruta where fecha_s = '2023-01-{}'""".format(dia_ec)
sql3 = """select * from control_radio.asignaciones_ruta where fecha_s = '{}'""".format(dia_ec)

asignaciones = pd.read_sql(sql3,con=conn)
asignaciones.drop(['id_asig', 'fecha_s'], axis=1, inplace=True)
asignaciones.columns = ['ID empresa','Empresa','Economico','Rotulo','Primera Ruta','Horario primer','Ultima Ruta','Horario ultimo','Fecha Generacion']


#LECTURA CAMIONES.CSV
rot1 = pd.read_csv('camiones.csv',header=1, usecols=[2,4,13],names=['tipo_ruta', 'Rotulo','msisdn']).dropna()

#LECTURA PARADEROS.CSV
rot2 = pd.read_csv('paraderos.csv',header=1, usecols=[1,2,11],names=['tipo_ruta', 'Rotulo','msisdn'])

#LECTURA DNS_MTY_IT1.xlsx
mty = pd.read_excel('DNS_MTY_IT1.xlsx', usecols=[1,3,4], names=['msisdn',  'tipo_ruta', 'Rotulo'])

#CONCAT DE ROTULOS
rotulos = pd.concat([rot1,rot2,mty]).dropna()

#MODIFICACIÓN DE rotulos'msisdn'
rotulos['msisdn'] = rotulos['msisdn'].apply(lambda x: str(int(x)))

rutas = rotulos[['tipo_ruta','msisdn']]
rotulos = rotulos[['Rotulo','msisdn']]

diccionario1 = rutas.set_index('msisdn').T.to_dict('list')
diccionario2 = rotulos.set_index('msisdn').T.to_dict('list')

final['MSISDN_2'] = final['MSISDN'].astype(str).str[2:]
final["Ruta"] = final["MSISDN_2"].map(diccionario1)
final["Rotulo"] = final["MSISDN_2"].map(diccionario2)

final['Rotulo'] = final['Rotulo'].astype(str)
final['Rotulo'] = final['Rotulo'].str[2:-2]


# In[16]:


#ASIGNACIÓN O NO DE MSISDN (ROUTER) CON BASE EN RÓTULO DE UNIDAD 

asignados = asignaciones.Rotulo.unique()

final.loc[final.Rotulo.astype(str).isin(asignados),'Asignado'] = 1 #sí asignado
final.loc[~final.Rotulo.astype(str).isin(asignados),'Asignado'] = 0 #no asignado

final['MSISDN'] = final['MSISDN'].astype(str).str[2:]


# #### Diferencias

# In[17]:


#DIFERENCIA AYER VS. ANTIER
#OBJETIVO DE PROCESO: Evaluar las conclusiones del día (anterior) VS. el el día anterior

#LECTURA DEBE SER:
anterior = pd.read_csv('ESTADOS_CONSUMO_HASTA_{}.csv'.format(dia_compr)) #recordar: dia_comp = ANTIER

set1 = set(anterior.MSISDN.astype(str).unique())
set2 = set(final.MSISDN.astype(str).unique())

si_anterior = list(set1.difference(set2))
si_actual = list(set2.difference(set1))

j_anterior = anterior.iloc[0]['Fecha Transaccion'][:2]
j_actual = final.iloc[0]['Fecha Transaccion'][:2]

dif_anterior = anterior[anterior.MSISDN.astype(str).isin(si_anterior)]
dif_actual = final[final.MSISDN.astype(str).isin(si_actual)]

print("Qué obtuve ANTIER vs AYER:",dif_anterior.shape)
print("Qué obtuve AYER vs ANTIER:",dif_actual.shape)

#EXPORTACIÓN A CSV EN CÓDIGO ORIGINAL 
dif_anterior.to_csv('diferencia/{}_diferencia_{}.csv'.format(j_anterior,j_actual), index=False, encoding='utf-8')
dif_actual.to_csv('diferencia/{}_diferencia_{}.csv'.format(j_actual,j_anterior), index=False, encoding='utf-8')


# In[18]:


print(anterior.iloc[0]['Fecha Transaccion'][:2])
print(final.iloc[0]['Fecha Transaccion'][:2])


# In[19]:


#DIFERENCIA AYER VS. EL MEJOR DÍA
anterior = pd.read_csv('ESTADOS_CONSUMO_HASTA_07022023.csv'.format(dia_comp))


set1 = set(anterior.MSISDN.astype(str).unique())
set2 = set(final.MSISDN.astype(str).unique())

si_anterior = list(set1.difference(set2))
si_actual = list(set2.difference(set1))

j_anterior = anterior.iloc[0]['Fecha Transaccion'][:2]
j_actual = final.iloc[0]['Fecha Transaccion'][:2]

dif_anterior = anterior[anterior.MSISDN.astype(str).isin(si_anterior)]
dif_actual = final[final.MSISDN.astype(str).isin(si_actual)]

print("Qué obtuve EL MEJOR DÍA vs AYER:",dif_anterior.shape)
print("Qué obtuve AYER vs EL MEJOR DÍA:",dif_actual.shape)

#EXPORTACIÓN A CSV EN CÓDIGO ORIGINAL
dif_anterior.to_csv('diferencia/{}febrero_diferencia_{}.csv'.format(j_anterior,j_actual), index=False, encoding='utf-8')
dif_actual.to_csv('diferencia/{}_diferencia_{}febrero.csv'.format(j_actual,j_anterior), index=False, encoding='utf-8')


# #### Bolsas futuras

# In[20]:


#OBJETIVO PROCESO: saber si tengo bolsas futuras en los DN o Router
#Para cada DN pregunto si tiene más de una bolsa

df_nuevo['MSISDN'] = df_nuevo['MSISDN'].astype(str).str[2:]

bolsas = df_nuevo.groupby('MSISDN')['Consumo Medido Acum RGU-UF y CI Edo Activo 10'].apply(list).reset_index()

lista=[]

dns_unicos = df_nuevo.MSISDN.unique().tolist()

for i in dns_unicos:
    tabla = df_nuevo[df_nuevo.MSISDN == i ] 
    bolsas_futuras = tabla[tabla['Consumo Medido Acum RGU-UF y CI Edo Activo 10']==0].shape[0] 
    if bolsas_futuras>=1: #Si tiene más de una bolsa en 0 == PROBLEMA 
        lista.append(i)
        
print(len(lista))

#Identifica si hay problema o no
final.loc[final.MSISDN.isin(lista), 'Problema'] = 'Si'
final.loc[~final.MSISDN.isin(lista), 'Problema'] = 'No'


# In[21]:


#Cuántas bolsas futuras tenemos para cada router (Bolsa_futuras= bolsas que no se han usado)
problema = df_nuevo[df_nuevo.MSISDN.isin(lista)]
problema = problema[problema['Consumo Medido Acum RGU-UF y CI Edo Activo 10'] == 0]

tabla = problema.groupby(['MSISDN'])['Consumo Medido Acum RGU-UF y CI Edo Activo 10'].apply(list).reset_index()
tabla['Bolsas_fututas'] = tabla['Consumo Medido Acum RGU-UF y CI Edo Activo 10'].astype(str).str.len()/3
tabla2 = tabla[['MSISDN',  'Bolsas_fututas']]

final['MSISDN'] = final['MSISDN'].astype(str)
tabla2['MSISDN'] = tabla2['MSISDN'].astype(str)

resultado = pd.merge(final,tabla2, on ='MSISDN',how='left')
resultado['Bolsas_fututas'] = resultado['Bolsas_fututas'].fillna(0)


# #### Ofertas

# In[22]:


final = resultado.copy()

#LLAMADA DE BD
conn = pymysql.connect(host='rdso.wiwi.services', port=3306, user='martin', passwd='nomepierdas')


sql2 = """SELECT * from usuarioswiwi.altan_ofertas 
 where fecha >= '2022-12-{} 00:00:00' and fecha <= '2022-12-{} 23:59:59'
 """.format(dia_ecd,dia_ecd)

data = pd.read_sql(sql2,con=conn)
#Lista de DN o routers
dns_oferta = data.event_id.unique().tolist()

#Si el DN aparece en la tabla "Altan ofertas" asigna un sí
final.loc[final.MSISDN.astype(str).isin(dns_oferta),'altan_ofertas'] = 1 
#Si el DN NO aparece en la tabla "Altan ofertas" asigna un no
final.loc[~final.MSISDN.astype(str).isin(dns_oferta),'altan_ofertas'] = 0


# #### Métricas 1.0

# In[23]:


final['oferta'] = final['Offer Name'].str[15:21] ## ------------------corregir**

#Equivalencia de Megas a GB
gbs_100 = {'40000M':39.0625,
           '10000M':9.7656,
           '20000M':19.5312,
           '30000M':29.2969,
           '5000M ':4.882813,
           '50000M':48.828125,
           'BE 250':0.232831}

#cuánto equivale la oferta al 100% en GB
final['100_pct'] = final["oferta"].map(gbs_100)

#cuánto equivale la oferta al 80% en GB
final['80_pct'] = round(final['100_pct']*0.8,4) 

#De lo consumido qué porcentaje lleva del 100%
final['Consumo'] = round((final['GB']/final['100_pct'])*100,4) 

#cuánto queda disponible en GB
final['Disponible'] = round((final['100_pct']-final['GB']),4) 

#promedio de: lo que se ha consumido/ días de bolsa activa 
final['Promedio'] = round((final['GB']/(final['Dias Edo Activo RGU y CI']+1)),4)

#cuánto es en 1 día de consumo
final['1_dia_de_consumo'] = round((final['Disponible']-final['Promedio']),4)

#cuánto es en 2 días de consumo
final['2_dias_de_consumo'] = round((final['Promedio']*2),4)

#
final['disponible_2_dias'] = round((final['Disponible']-final['2_dias_de_consumo']),4)


# #### Promedio con los 30 días

# In[24]:


final['Consumo_30d'] = final['Promedio']*30

final['Bolsas_50GB'] = 0

final['Bolsas'] = 0

df_menos50 = final.loc[final.Consumo_30d <= 50]
df_menos50 = df_menos50.reset_index(drop=True)
df_mas50 = final.loc[final.Consumo_30d > 50]
df_mas50 = df_mas50.reset_index(drop=True)


# In[25]:


#Función escalonada, porque las ofertas van brincando de 5 en 5, por lo que no es función continua. 
for index, row in df_menos50.iterrows():

    if row['Consumo_30d'] == 0:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 250 mg'
    
    elif 0 < row['Consumo_30d'] <= 5:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 5 GB'
        
    elif 5 < row['Consumo_30d'] <= 10:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 10 GB'
        
    elif 10 < row['Consumo_30d'] <= 15:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 10 GB y 1 Bolsa de 5GB'
    
    elif 15 < row['Consumo_30d'] <= 20:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 20 GB'
        
    elif 20 < row['Consumo_30d'] <= 25:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 20 GB y 1 Bolsa de 5GB'
    
    elif 25 < row['Consumo_30d'] <= 30:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 30 GB'
    
    elif 30 < row['Consumo_30d'] <= 35:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 30 GB y 1 Bolsa de 5GB'
    
    elif 35 < row['Consumo_30d'] <= 40:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 40 GB'
    
    elif 40 < row['Consumo_30d'] <= 45:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 40 GB y 1 Bolsa de 5GB'
    
    elif 45 < row['Consumo_30d'] <= 50:
        df_menos50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 50 GB'


# In[26]:


for index, row in df_mas50.iterrows():
    
    bolsas_enteras  = int(row['Consumo_30d'] / 50)
    df_mas50.iloc[index,int(final.shape[1])-2] = bolsas_enteras
    
    residuo = row['Consumo_30d'] % 50
    
    if residuo == 0:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 250 mg'
    
    elif 0 < residuo <= 5:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 5 GB'
        
    elif 5 < residuo <= 10:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 10 GB'
        
    elif 10 < residuo <= 15:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 10 GB y 1 Bolsa de 5GB'
    
    elif 15 < residuo <= 20:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 20 GB'
        
    elif 20 < residuo <= 25:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 20 GB y 1 Bolsa de 5GB'
    
    elif 25 < residuo <= 30:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 30 GB'
    
    elif 30 < residuo <= 35:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 30 GB y 1 Bolsa de 5GB'
    
    elif 35 < residuo <= 40:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 40 GB'
    
    elif 40 < residuo <= 45:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 40 GB y 1 Bolsa de 5GB'
    
    elif 45 < residuo <= 50:
        df_mas50.iloc[index,int(final.shape[1])-1] = '1 Bolsa de 50 GB'


# In[27]:


resultado2 = pd.concat([df_menos50,df_mas50])


# #### Promedio con los 30 días

# In[28]:


ultimos30 = pd.read_csv('Promedios_ultimos30d_08022022.csv')


# In[29]:


ultimos30['MSISDN'] = ultimos30['MSISDN'].astype(str)
resultado2 = pd.merge(resultado2, ultimos30, on='MSISDN',how='left')


# In[30]:


resultado2['Promedio_ulti30ds'] = resultado2['Promedio_ulti30ds'].fillna(0)


# In[31]:


resultado2['Bolsas_50GB_B'] = 0

resultado2['Bolsas_B'] = 0

df_menos50 = resultado2.loc[resultado2.Promedio_ulti30ds <= 50]
df_menos50 = df_menos50.reset_index(drop=True)
df_mas50 = resultado2.loc[resultado2.Promedio_ulti30ds > 50]
df_mas50 = df_mas50.reset_index(drop=True)


# In[32]:


for index, row in df_menos50.iterrows():

    if row['Promedio_ulti30ds'] == 0:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 250 mg'
    
    elif 0 < row['Promedio_ulti30ds'] <= 5:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 5 GB'
        
    elif 5 < row['Promedio_ulti30ds'] <= 10:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 10 GB'
        
    elif 10 < row['Promedio_ulti30ds'] <= 15:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 10 GB y 1 Bolsa de 5GB'
    
    elif 15 < row['Promedio_ulti30ds'] <= 20:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 20 GB'
        
    elif 20 < row['Promedio_ulti30ds'] <= 25:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 20 GB y 1 Bolsa de 5GB'
    
    elif 25 < row['Promedio_ulti30ds'] <= 30:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 30 GB'
    
    elif 30 < row['Promedio_ulti30ds'] <= 35:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 30 GB y 1 Bolsa de 5GB'
    
    elif 35 < row['Promedio_ulti30ds'] <= 40:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 40 GB'
    
    elif 40 < row['Promedio_ulti30ds'] <= 45:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 40 GB y 1 Bolsa de 5GB'
    
    elif 45 < row['Promedio_ulti30ds'] <= 50:
        df_menos50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 50 GB'


# In[33]:


df_menos50.iloc[0,int(resultado2.shape[1])-1]


# In[34]:


for index, row in df_mas50.iterrows():
    
    bolsas_enteras  = int(row['Promedio_ulti30ds'] / 50)
    df_mas50.iloc[index,int(resultado2.shape[1])-2] = bolsas_enteras
    
    residuo = row['Promedio_ulti30ds'] % 50
   
    if residuo == 0:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 250 mg'
    
    elif 0 < residuo <= 5:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 5 GB'       
        
    elif 5 < residuo <= 10:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 10 GB'
        
    elif 10 < residuo <= 15:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 10 GB y 1 Bolsa de 5GB'
    
    elif 15 < residuo <= 20:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 20 GB'
        
    elif 20 < residuo <= 25:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 20 GB y 1 Bolsa de 5GB'
    
    elif 25 < residuo <= 30:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 30 GB'
    
    elif 30 < residuo <= 35:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 30 GB y 1 Bolsa de 5GB'
    
    elif 35 < residuo <= 40:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 40 GB'
    
    elif 40 < residuo <= 45:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 40 GB y 1 Bolsa de 5GB'
    
    elif 45 < residuo <= 50:
        df_mas50.iloc[index,int(resultado2.shape[1])-1] = '1 Bolsa de 50 GB'


# In[35]:


resultado3 = pd.concat([df_menos50,df_mas50])


# ### Actividad 90 días

# In[36]:


estados['MSISDN'] = estados['MSISDN'].astype(str).str[2:]
estados['Fecha Transaccion'] = pd.to_datetime(estados['Fecha Transaccion'], format = '%d/%m/%Y')
estados['Fecha Medicion'] =  pd.to_datetime(estados['Fecha Medicion'],  format = '%d/%m/%Y')


# In[37]:


grupo_A = resultado3[resultado3.Caso == 'A']
dns_A = grupo_A.MSISDN.astype(str).unique()

print(len(dns_A))

dns_3meses = []

for i in dns_A:
    tabla = estados[estados.MSISDN == i]
    cota_inferior = tabla['Fecha Transaccion'].max() - timedelta(days = 90)
    tabla2 = tabla[tabla['Fecha Transaccion'] <= cota_inferior]
    consumos = tabla2['Consumo Medido Acum RGU-UF y CI Edo Activo 10'].astype(int).sum()
    
    if consumos != 0:
        dns_3meses.append(i)


# In[38]:


resultado3['Actividad_90dias'] = 0
print(resultado3.shape)

resultado3.loc[resultado3.MSISDN.isin(dns_3meses),'Actividad_90dias'] = 1


# #### Nombre de la empresa

# In[39]:


rot1 = pd.read_csv('camiones.csv',header=1, usecols=[1,13])

mty = pd.read_excel('DNS_MTY_IT1.xlsx', usecols=[2,1], names=['msisdn','NOMBRE CORTO EMPRESA'])
mty = mty.reindex(columns=['NOMBRE CORTO EMPRESA','msisdn'])

empresas = pd.concat([rot1,mty]).dropna()


# In[40]:


diccionario3 = empresas.set_index('msisdn').T.to_dict('list')

resultado3["Nombre_empresa"] = resultado3["MSISDN_2"].astype(float).map(diccionario3)


# In[41]:


resultado3.to_csv('ESTADOS_CONSUMO_HASTA_{}.csv'.format(dia_ecr), index=False, encoding='utf-8')


# In[42]:


final = time.time()
tiempo = (final - inicio)/60
print(f'Tiempo transcurrido: {tiempo:.3f} minutos')

