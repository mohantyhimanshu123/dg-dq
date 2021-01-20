from django.shortcuts import render, HttpResponse, redirect
from .models import csv_upload, csvfileupload
import pandas as pd
import os
from django.contrib import messages
from django.views.generic import TemplateView
import csv
import zipfile
import re
import numbers
import decimal
import string
import collections as ct
from email_validator import validate_email, EmailNotValidError
from django.core.files.storage import FileSystemStorage
from fuzzywuzzy import fuzz, process
from pyzipcode import ZipCodeDatabase
from pandas import *
import re
from commonregex import CommonRegex
from validate_email import validate_email
from sklearn.ensemble import IsolationForest
from collections import Counter 
from statistics import *
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
import io


file2 = 0
# Create your views here.
def main(request):
    return render(request, 'main.html')

def exp(request):
    return render(request, 'exp.html')
def single(request):
    return render(request, 'single.html')
def multiple(request):
    return render(request, 'multiple.html')

def home(request):
    if request.method == "POST":
        global file2 
        file2 = request.FILES["file"]
        document = csv_upload.objects.create(file=file2)
        document.save()
        if f'{file2}'.endswith('.csv'):
            df = pd.read_csv(f'media/{file2}')
            format_table = formatting(df)
            nm = 'newcsv_format.csv'
            format_table.to_csv(f'media/extracts/{nm}')
            return render(request, 'analyze1.html')
        else:
          with zipfile.ZipFile(f'media/{file2}', 'r') as my_zip:
            my_zip.extractall('media/extracts')
            csv1 = pd.read_csv(f'media/extracts/{my_zip.namelist()[0]}')
            csv2 = pd.read_csv(f'media/extracts/{my_zip.namelist()[1]}')
            for cl in csv1.columns:
                if cl in csv2.columns:
                    csv3 = pd.merge(csv1, csv2, how = 'outer')
                    nm = 'newcsv.csv'
                    csv3.to_csv(f'media/extracts/{nm}')
                    csv3 = pd.read_csv('media/extracts/newcsv.csv')
                    format_table3 = formatting(csv3)
                    format_table3.to_csv('media/extracts/newcsv_format3.csv')
            else:
                    csv3 = pd.concat([csv1, csv2], axis = 1)
                    nm = 'newcsv.csv'
                    csv3.to_csv(f'media/extracts/{nm}')
                    csv3 = pd.read_csv('media/extracts/newcsv.csv')
                    format_table3 = formatting(csv3)
                    format_table3.to_csv('media/extracts/newcsv_format3.csv')
          return render(request, 'analyze2.html')
    return render(request, 'header.html')

def raw(request):
		 global file2 
		 csv_fp = open(f'media/{file2}', 'r')
		 reader = csv.DictReader(csv_fp)
		 headers = [col for col in reader.fieldnames]
		 out = [row for row in reader]
		 return render(request, 'raw.html', {'data': out, 'headers': headers, 'raw': csv_fp})

def table(request):
		 global file2 
		 csv_fp = open(f'media/{file2}', 'r')
		 reader = csv.DictReader(csv_fp)
		 headers = [col for col in reader.fieldnames]
		 out = [row for row in reader]
		 return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'FILE ', 'download': f'media/{file2}'})


def zipper():
     global file2
     with zipfile.ZipFile(f'media/{file2}', 'r') as my_zip:
           return my_zip.namelist()[0], my_zip.namelist()[1]
           
def csv1(request):
			global file2
			with zipfile.ZipFile(f'media/{ file2 }', 'r') as my_zip:
					my_zip.extractall('media/extracts')
					csv_fp = open(f'media/extracts/{my_zip.namelist()[0]}', 'r')
					reader = csv.DictReader(csv_fp)
					headers = [col for col in reader.fieldnames]
					out = [row for row in reader]
					return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'FILE 1', 'download': f'media/extracts/{my_zip.namelist()[0]}'})

def csv2(request):
			global file2
			with zipfile.ZipFile(f'media/{ file2 }', 'r') as my_zip:
					my_zip.extractall('media/extracts')
					csv_fp = open(f'media/extracts/{my_zip.namelist()[1]}', 'r')
					reader = csv.DictReader(csv_fp)
					headers = [col for col in reader.fieldnames]
					out = [row for row in reader]
					return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'FILE 2', 'download': f'media/extracts/{my_zip.namelist()[1]}'})
			
def merger(request):
	global file2
	with zipfile.ZipFile(f'media/{file2}', 'r') as my_zip:
		my_zip.extractall('media/extracts')
		csv1 = pd.read_csv(f'media/extracts/{my_zip.namelist()[0]}')
		csv2 = pd.read_csv(f'media/extracts/{my_zip.namelist()[1]}')
		for cl in csv1.columns:
			if cl in csv2.columns:
				csv3 = pd.merge(csv1, csv2, how = 'outer')
				nm = 'newcsv.csv'
				csv3.to_csv(f'media/extracts/{nm}') 
				my_csv = open(f'media/extracts/{nm}', 'r')
				reader = csv.DictReader(my_csv)
				headers = [ col for col in reader.fieldnames]
				out = [ row for row in reader]
		else:
			csv3 = pd.concat([csv1, csv2], axis = 1)
			nm = 'newcsv.csv'
			csv3.to_csv(f'media/extracts/{nm}', index = False)
			my_csv = open(f'media/extracts/{nm}', 'r')
			reader = csv.DictReader(my_csv)
			headers = [col for col in reader.fieldnames]
			out = [row for row in reader]
		return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'merger', 'download': 'media/extracts/newcsv.csv'})

def format_table(request):
    global file2
    if f'{file2}'.endswith('.csv'):
        my_csv = open('media/extracts/newcsv_format.csv', 'r')
        reader = csv.DictReader(my_csv)
        headers = [col for col in reader.fieldnames]
        out = [row for row in reader]
        nm = 'newcsv_format'
    else:
        my_csv = open(f'media/extracts/newcsv_format3.csv', 'r')
        reader = csv.DictReader(my_csv)
        headers = [col for col in reader.fieldnames]
        out = [row for row in reader]
        nm = 'newcsv_format3'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/extracts/{nm}'})

def formatting(Datadf):
    data = pd.DataFrame()
    data['Attributes'] = Datadf.columns
    df = pd.DataFrame()
    df = Datadf.describe(include='all')
    df = df.transpose()
    dc = df['count']
    du = Datadf.nunique().to_list()
    dfr = 0
    data['Count'] = 0
    data['Unique'] = 0
    data['Frequency'] = 0
    data['DataTypes'] = 0
    data['IsNull'] = 0
    data['IsNotNull'] = 0
    data['PercentageNull'] = 0

    for i in range(len(Datadf.columns)):
        data['Count'][i] = dc[i]
        data['Unique'][i] = du[i]
        #data['Frequency'][i] = dfr[i]
    ddt = Datadf.dtypes
    disn = Datadf.isnull().sum()
    disnn = Datadf.shape[0] - Datadf.isnull().sum()
    dpnl = (Datadf.isnull().sum()/Datadf.shape[0]) * 100
    for i in range(len(Datadf.columns)):
        data['DataTypes'][i] = ddt[i]
        data['IsNull'][i] = disn[i]
        data['IsNotNull'][i] = disnn[i]

    count = 0
    count1 = 0
    count2 = 0
    c = []
    c1 = []
    c2 = []
    data['Entire_Upper'] = 0
    data['Entire_Lower'] = 0
    for column in Datadf.columns:
        for i in range(len(Datadf)):
            if(str(Datadf[column][i]).isupper()):
                count = count+1
            if(str(Datadf[column][i]).islower()):
                count1 = count1+1
        c.append(count)
        c1.append(count1)
        count = 0
        count1 = 0

    data['Entire_Upper'] = c
    data['Entire_Lower'] = c1

    data['WithSpaces'] = 0
    data['WithoutSpaces'] = 0

    for column in Datadf.columns:
        for i in range(len(Datadf)):
            a = str(Datadf[column][i])
            t = a.split(" ")
            if len(t) > 1:
                count2 = count2+1
        c2.append(count2)
        count2 = 0

    data['WithSpaces'] = c2
    data['WithoutSpaces'] = Datadf.shape[0]-data['WithSpaces']

    data['Duplicates'] = data['Count'] - data['Unique']

    data['Min Words'] = 0
    data['Max Words'] = 0

    max = 0
    min = 9999
    c3 = []
    c4 = []
    for column in Datadf.columns:
        for i in range(len(Datadf)):
            a = str(Datadf[column][i])
            t = a.split(" ")
            if(len(t) > max):
                max = len(t)
            if(len(t) < min):
                min = len(t)
        c3.append(min)
        c4.append(max)
        max = 0
        min = 9999

    data['Min Words'] = c3
    data['Max Words'] = c4

    data['Min Length'] = 0
    data['Max Length'] = 0

    max = 0
    min = 9999
    c5 = []
    c6 = []
    for column in Datadf.columns:
        for i in range(len(Datadf)):
            a = str(Datadf[column][i])
            t1 = len(a)
            if(t1 > max):
                max = t1
            if(t1 < min):
                min = t1
        c5.append(min)
        c6.append(max)
        max = 0
        min = 9999

    data['Min Length'] = c5
    data['Max Length'] = c6

    data['Total Character count'] = 0
    data['Average Character count'] = 0

    c7 = []
    charcount = 0
    for column in Datadf.columns:
        for i in range(len(Datadf)):
            a = str(Datadf[column][i])
            charcount = charcount + len(a)
        c7.append(charcount)
        charcount = 0

    data['Total Character count'] = c7
    data['Average Character count'] = (
        data['Total Character count']/Datadf.shape[0])

    data['Trailing White Space'] = 0

    c8 = []
    spacecount = 0
    for column in Datadf.columns:
        for i in range(len(Datadf)):
            a = str(Datadf[column][i])
            if(a.endswith(' ')):
                spacecount = spacecount + 1
        c8.append(spacecount)
        spacecount = 0

    data['Trailing White Space'] = c8

    data['Entire White Space'] = 0
    data['Percentage Entire White Space'] = 0

    c9 = []
    spcount = 0
    for column in Datadf.columns:
        for i in range(len(Datadf)):
            if(str(Datadf[column][i]).isspace()):
                spcount = spcount+1
        c9.append(spcount)
        spcount = 0

    data['Entire White Space'] = c9
    data['Percentage Entire White Space'] = (
        data['Entire White Space']/Datadf.shape[0]) * 100

    data['Percentage Numeric'] = 0

    numcount = 0
    k = []
    for column in Datadf.columns:
        for i in range(len(Datadf)):
            if(isinstance(Datadf[column][i], numbers.Number)):
                numcount = numcount + 1
        k.append((numcount/Datadf.shape[0]) * 100)
        numcount = 0
    data['Percentage Numeric'] = k

    data['Percentage AlphaNumeric'] = 0

    charcount = 0
    k1 = []
    for column in Datadf.columns:
        for i in range(len(Datadf)):
            a = str(Datadf[column][i])
            if(a.isalnum()):
                charcount = charcount + 1
        k1.append((charcount/Datadf.shape[0]) * 100)
        charcount = 0
    data['Percentage AlphaNumeric'] = k1

    data['SpecialCharacters'] = 0

    n = 0
    c10 = []
    special_chars = ['$', '#', ',', '+', '*', '&', '^',
                     '%', '_', '-', '=', '@', '!', '(', ')', '~']
    for column in Datadf.columns:
        for i in range(len(Datadf)):

            st = str(Datadf[column][i])
            n = n + sum(v for k, v in ct.Counter(st).items()
                        if k in special_chars)
        c10.append(n)
        n = 0

    data['SpecialCharacters'] = c10
    order = ['Attributes',
             'DataTypes',
             'Count',
             'Unique',
             'Duplicates',
             'Frequency',
             'IsNull',
             'IsNotNull',
             'PercentageNull',
             'Entire_Upper',
             'Entire_Lower',
             'WithSpaces',
             'WithoutSpaces',
             'Min Words',
             'Max Words',
             'Min Length',
             'Max Length',
             'Total Character count',
             'Average Character count',
             'Trailing White Space',
             'Entire White Space',
             'Percentage Entire White Space',
             'Percentage Numeric',
             'Percentage AlphaNumeric',
             'SpecialCharacters']

    data = data[order]
    #data.to_html('templates\\Analysis.html')  
    return data
		
def PatternRecog(Datadf, data): #pattern analysis function, dataset and profiling results as parameter
  pattern = []
  def change_char(s, p, r):
    return s[:p]+r+s[p+1:]
  
  for i in range(len(data)):
    if(data['Min Length'][i] - data['Max Length'][i] <= 9):
      pattern.append(data['Attributes'][i])
  
  Pattern = pd.DataFrame()
  Pattern = Datadf
  Pattern['column'] = ''
  Pattern['pattern'] = ''
  for patterncols in pattern:
    for j in range(len(Datadf)):
      a = str(Datadf.loc[Datadf.index[j] ,patterncols])
      for k in range(len(a)):
        if(a[k].isnumeric()):
          a = change_char(a, k, '9')
        if(a[k].isalpha()):
          a = change_char(a, k, 'A')
                    
      Pattern.loc[Pattern.index[j], 'column'] = Pattern.loc[Pattern.index[j], 'column'] + ' , ' + patterncols
      Pattern.loc[Pattern.index[j], 'pattern'] = Pattern.loc[Pattern.index[j], 'pattern'] + ' , ' + a
  
  for j in range(len(Datadf)):
    k = str(Pattern.loc[Pattern.index[j], 'column'])
    l = str(Pattern.loc[Pattern.index[j], 'pattern'])    
    Pattern.loc[Pattern.index[j], 'column'] = k[2:]
    Pattern.loc[Pattern.index[j], 'pattern'] = l[2:]
    
    new = Pattern['pattern'].str.split(' , ', n = 15, expand = True) 
    
  return new, pattern
  
#phone number pattern
def operations_phone(data, Pattern):
  #phone number pattern
  phone_no = []
  phone = []
  not_phone_no = []
  th = []
  mail = []
  for i in Pattern.columns:
    for j in range(len(Pattern[i])):
      if fuzz.ratio('999-999-9999', Pattern[i][j]) == 100 or fuzz.ratio('99999-99999', Pattern[i][j]) == 100 or fuzz.ratio('9999999999', Pattern[i][j]) == 100 or fuzz.ratio('99999-999999', Pattern[i][j]) == 100:
        #saving identified mail ids
        phone.append(data[i][j])
        phone_no.append([data[Pattern.columns[0]][j],data[i][j]])
        if i not in mail:
          mail.append(i)
  #daf = DataFrame(phone_no, columns = ['cust_id', 'phone_no'])
  #daf.to_csv('phone.csv')
  for i in mail:
    for j in range(len(data[i])):
      if data[i][j] not in phone:
        not_phone_no.append([data[Pattern.columns[0]][j],data[i][j]])
        
  phone_no = DataFrame (phone_no,columns=['cust_id','Correct_Phone_no'])
  not_phone_no = DataFrame (not_phone_no,columns=['cust_id','Anomoly_Phone_no'])
  
  return phone_no, not_phone_no

  

#mail pattern Checking
def operations_mail(data, Pattern):
  #mail pattern
  mail_id = []
  not_mail_id = []
  mail = []
  for i in range(len(Pattern.columns)):
    p = 0
    for j in range(len(Pattern[Pattern.columns[i]])):
      t = fuzz.partial_ratio('A@A', Pattern[Pattern.columns[i]][j])
      if t == 100:
        try:
          if validate_email(data[Pattern.columns[i]][j]):
            p+=1
            #saving identified mail id patterns
            mail_id.append(data[Pattern.columns[i]][j])
            if Pattern.columns[i] not in mail:
              mail.append(Pattern.columns[i])
        except:
          pass
  for i in mail:
    for j in range(len(data[i])):
        if data[i][j] not in mail_id:
          not_mail_id.append(data[i][j])
          
  mail_id = DataFrame (mail_id,columns=['Correct_mail'])
  not_mail_id = DataFrame (not_mail_id,columns=['Anomoly_mail_id'])
  return mail_id, not_mail_id

def operations_zip(data, Pattern):
  #zip code pattern
  mail = []
  zip_code = []
  not_zip_code = []
  for i in Pattern.columns:
    for j in range(len(Pattern[i])):
      if fuzz.ratio('9999', Pattern[i][j]) == 100 or fuzz.ratio('99999', Pattern[i][j]) == 100:
        #saving identified zip codes
          if i not in mail:
            mail.append(i)
          zcdb = ZipCodeDatabase()
          if len(str(data[i][j]))==5: #and zcdb[int(data[i][j])].zip != None:
            zip_code.append(data[i][j])
          elif len(str(data[i][j]).split('.')[0])==4:
            nn = str(0)+str(data[i][j])
            #if zcdb[int(nn)].zip != None:
            zip_code.append(data[i][j])
          else:
            pass
  for i in mail:
    for j in range(len(data[i])):
      if data[i][j] not in zip_code:
        not_zip_code.append(data[i][j])
        
  zip_code = DataFrame (zip_code,columns=['Correct_zip_code'])
  not_zip_code = DataFrame (not_zip_code,columns=['Anomoly_zip_code'])
  return zip_code, not_zip_code

# address pattern
def operations_address(data, Pattern):
  address = []
  not_address = []
  mail = []
  text = CommonRegex()
  for i in range(len(Pattern.columns)):
    for j in range(len(Pattern[Pattern.columns[i]])):
        t = fuzz.partial_ratio('9 A', Pattern[Pattern.columns[i]][j])
        try:
            if t == 100 and text.street_addresses(re.sub('[^A-Za-z0-9]+', ' ', data[Pattern.columns[i]][j])):
              if Pattern.columns[i] not in mail:
                mail.append(Pattern.columns[i])
              address.append(data[Pattern.columns[i]][j])
        except:
            pass
  for i in mail:
    for j in range(len(data[i])):
      if data[i][j] not in address:
          not_address.append(data[i][j])
          
  address = DataFrame (address,columns=['Correct_Address'])
  not_address = DataFrame (not_address,columns=['Anomoly_Address'])
  return address, not_address

def analyze(request):
    global file2
    
    if f'{file2}'.endswith('.csv'):
        dt1 = pd.read_csv(f'media/{file2}')
        dt2 = pd.read_csv(f'media/extracts/newcsv_format.csv')
        #Anomaly detection for single dataset:
        Pattern, cols= PatternRecog(dt1, dt2)
        Pattern.columns = cols

        phone_no, not_phone_no = operations_phone(dt1, Pattern)
        phone_no.to_csv('media/not anomaly/phone.csv')
        not_phone_no.to_csv('media/anomaly/phone_a.csv')

        zip_code, not_zip_code = operations_zip(dt1, Pattern)
        zip_code.to_csv('media/not anomaly/zip.csv')
        not_zip_code.to_csv('media/anomaly/zip_a.csv')

        mail_id, not_mail_id = operations_mail(dt1, Pattern)
        mail_id.to_csv('media/not anomaly/email.csv')
        not_mail_id.to_csv('media/anomaly/email_a.csv')

        address, not_address = operations_address(dt1, Pattern)
        address.to_csv('media/not anomaly/address.csv')
        not_address.to_csv('media/anomaly/address_a.csv')
        return render(request, 'single.html')
    else:
        zipfile1, zipfile2 = zipper()
        zip1 = pd.read_csv(f'media/extracts/{zipfile1}')
        zip2 = pd.read_csv(f'media/extracts/{zipfile2}')
        zip3 = pd.read_csv(f'media/extracts/newcsv.csv')
        zip_format3 = pd.read_csv(f'media/extracts/newcsv_format3.csv')
        Pattern3, cols3= PatternRecog(zip3, zip_format3)
        Pattern3.columns = cols3

        phone_no, not_phone_no = operations_phone(zip3, Pattern3)
        phone_no.to_csv('media/not anomaly/phone3.csv')
        not_phone_no.to_csv('media/anomaly/phone_a3.csv')

        zip_code, not_zip_code = operations_zip(zip3, Pattern3)
        zip_code.to_csv('media/not anomaly/zip3.csv')
        not_zip_code.to_csv('media/anomaly/zip_a3.csv')

        mail_id, not_mail_id = operations_mail(zip3, Pattern3)
        mail_id.to_csv('media/not anomaly/email3.csv')
        not_mail_id.to_csv('media/anomaly/email_a3.csv')

        address, not_address = operations_address(zip3, Pattern3)
        address.to_csv('media/not anomaly/address3.csv')
        not_address.to_csv('media/anomaly/address_a3.csv')
        return render(request, 'multiple.html')
            


def phone1(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    procsv=open('media/not anomaly/phone.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'phone.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/not anomaly/{nm}'})
  else:
    procsv=open('media/not anomaly/phone3.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'phone3.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/not anomaly/{nm}'})
    
def phone_a1(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    procsv=open('media/anomaly/phone_a.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'phone_a.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/anomaly/{nm}'})
  else:
    procsv=open('media/anomaly/phone_a3.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'phone_a3.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/anomaly/{nm}'})

def zip1(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    procsv=open('media/not anomaly/zip.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'zip.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/not anomaly/{nm}'})
  else:
    procsv=open('media/not anomaly/zip3.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'zip3.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/not anomaly/{nm}'})
    
def zip_a1(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    procsv=open('media/anomaly/zip_a.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'zip_a.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/anomaly/{nm}'})
  else:
    procsv=open('media/anomaly/zip_a3.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'zip_a3.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/anomaly/{nm}'})
    
def email1(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    procsv=open('media/not anomaly/email.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'email.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/not anomaly/{nm}'})
  else:
    procsv=open('media/not anomaly/email3.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'email3.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/not anomaly/{nm}'})

def email_a1(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    procsv=open('media/anomaly/email_a.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'email_a.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/anomaly/{nm}'})
  else:
    procsv=open('media/anomaly/email_a3.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'email_a3.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/anomaly/{nm}'})
      
def address1(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    procsv=open('media/not anomaly/address.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'address.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/not anomaly/{nm}'})
  else:
    procsv=open('media/not anomaly/address3.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'address3.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/not anomaly/{nm}'})

def address_a1(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    procsv=open('media/anomaly/address_a.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'address_a.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/anomaly/{nm}'})
  else:
    procsv=open('media/anomaly/address_a3.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'address_a3.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/anomaly/{nm}'})
      

#columnwise outliers 
# taking inter quartile range
def iqr(data):
  Q1 = data.quantile(0.25)
  Q3 = data.quantile(0.75)
  IQR = Q3 - Q1
  low_lim = Q1 - 1.5 * IQR 
  up_lim = Q3 + 1.5 * IQR
  return low_lim, up_lim

# function for finding mode
def most_frequent(List): 
	occurence_count = Counter(List) 
	return occurence_count.most_common(1)[0][0] 

# finding mean median mode	
def mmm(data, col):
  daa = []
  for i in col:
      me = mean(data[i])
      med = median(data[i])
      if len(data[i])-len(data[i].drop_duplicates())!=0:
        mod = most_frequent(data[i])
      else:
        mod = 'Nan'
      daa.append([me, med, mod])
  return daa

# finding outlier
def operations_outlier(data):
  cols = [i for i in data.columns]
  column = []
  cols_dtype = [i for i in data.dtypes]
  for i,j in zip(cols, cols_dtype):
    if j!='object':
      column.append(i)
  data[column] = data[column].fillna(0)
  col_name, col_outlier = [],  []
  for i in column:
    low, high = iqr(data[i])
    outlier = [] 
    for x in data[i]: 
      if ((x>= high) or (x<= low)): 
          outlier.append(x) 
    i_out = []
    i_out = list(set(outlier))
    col_name.append(i)
    col_outlier.append(i_out)
    mem = mmm(data[col_name], col_name)
  return(col_name, col_outlier, mem)

#total dataset outlier
def totaloutlier(data):
    column = []
    cols_dtype = [i for i in data.dtypes]
    for i,j in zip(data.columns, cols_dtype):
      if j!='object':
        column.append(i)
    data[column] = data[column].fillna(0)

    model=IsolationForest(n_estimators=50, max_samples='auto', contamination=float(0.05),max_features=1.0)
    model.fit(data[column])

    data['scores']=model.decision_function(data[column])
    data['outliers']=model.predict(data[column])

    anomaly=data.loc[data['outliers']==-1]
    anomaly_index=list(anomaly.index)
    #print(anomaly)
    df2 = DataFrame (anomaly)

    return(df2)


def op_out(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    dt1 = pd.read_csv(f'media/{file2}')
    col, dat, stats = operations_outlier(dt1)
    df1 = pd.DataFrame([dat])
    df1 = df1.transpose()
    df = pd.DataFrame(stats)
    # col gives columns with outliers
    # dat gives the data of outliers found
    # stats gives [mean, median, mode] in list form of all the outliers
    df1.index = col
    df.index = col
    df.index.name, df1.index.name = 'column', 'column'
    df.columns = ['mean', 'median', 'mode']
    df1.columns = ['outlier']
    df2 = pd.merge(df, df1, on = ['column'])
    df2.to_csv(f'media/op_out/col_out.csv')
    procsv=open('media/op_out/col_out.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'col_out.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/op_out/{nm}'})
  else:
    dt1 = pd.read_csv(f'media/extracts/newcsv.csv')
    col, dat, stats = operations_outlier(dt1)
    df1 = pd.DataFrame([dat])
    df1 = df1.transpose()
    df = pd.DataFrame(stats)
    # col gives columns with outliers
    # dat gives the data of outliers found
    # stats gives [mean, median, mode] in list form of all the outliers
    df1.index = col
    df.index = col
    df.index.name, df1.index.name = 'column', 'column'
    df.columns = ['mean', 'median', 'mode']
    df1.columns = ['outlier']
    df2 = pd.merge(df, df1, on = ['column'])
    df2.to_csv(f'media/op_out/col_out1.csv')
    procsv=open('media/op_out/col_out1.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'col_out1.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/op_out/{nm}'})

def total_out(request):
  global file2
  if f'{file2}'.endswith('.csv'):
    dt1 = pd.read_csv(f'media/{file2}')
    df2 = totaloutlier(dt1)
    df2.to_csv('media/total_out/total_out.csv')
    procsv=open('media/total_out/total_out.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'total_out.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/total_out/{nm}'})
  else:
    dt1 = pd.read_csv('media/extracts/newcsv.csv')
    df2 = totaloutlier(dt1)
    df2.to_csv('media/total_out/total_out1.csv')
    procsv=open('media/total_out/total_out1.csv','r')
    reader = csv.DictReader(procsv)
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    nm = 'total_out1.csv'
    return render(request, 'csvdatatable.html', {'data': out, 'headers': headers, 'name': 'INSIGHTS', 'download': f'media/total_out/{nm}'})

'''
Here we have worker for the data lineage part
'''
def data_lineage1(csv1, csv2):
    #Create a new dataframe to hold the final result and concat product
    new_df1, new_df2 = [], []
    dicti = {}# This is to show the matching columns
    l = []
    concat_df1, concat_df2 = [], []
    for i in csv1.columns:
      for j in csv2.columns:
        # Create a dumy dataframe to check whether two columns match or not 
        dummy_df = csv1[ csv1[i].eq(csv2[j]) == True]
        # Create a variable for the lengths of the dummy dataframe and the original dataframe
        l1, l2 = len(csv1[i]), len(dummy_df)
        if l2 >= (0.8*l1): # This shows that these columns match with each other with tolerance 80%
          concat_df_1 = pd.DataFrame((csv1[csv1[i].eq(csv2[j]) == False])[i])
          concat_df_2 = pd.DataFrame((csv2[csv1[i].eq(csv2[j]) == False])[j])
          if len(dicti) == 0:
                l.append(j)
                dicti = {i : l}
          else:
                l.append(j)
                dicti.update({i:l})
          if len(new_df1) == 0 and len(new_df2) == 0:
            new_df1, new_df2 = concat_df_1, concat_df_2
          else:
            new_df1 = pd.concat([new_df1, concat_df_1])
            new_df2 = pd.concat([new_df2, concat_df_2])
      l = []
    new_df1.to_csv(f'media/data_lineage/1n2.csv')
    new_df2.to_csv(f'media/data_lineage/2n1.csv')
    return dicti

def data_lineage(request):
   global file2
   with zipfile.ZipFile(f'media/{file2}', 'r') as my_zip:
            my_zip.extractall('media/extracts')
            csv1 = pd.read_csv(f'media/extracts/{my_zip.namelist()[0]}')
            csv2 = pd.read_csv(f'media/extracts/{my_zip.namelist()[1]}')
            dicti = data_lineage1(csv1, csv2)
            csv1n2 = open('media/data_lineage/1n2.csv','r')
            csv2n1 = open('media/data_lineage/2n1.csv','r')
            reader1 = csv.DictReader(csv1n2)
            reader2 = csv.DictReader(csv2n1)
            headers1 = [col1 for col1 in reader1.fieldnames]
            headers2 = [col2 for col2 in reader2.fieldnames]
            out1 = [row1 for row1 in reader1]
            out2 = [row2 for row2 in reader2]
            nm1 = '1n2.csv'
            nm2 = '2n1.csv'
            return render(request, 'data_lineage.html', 
            {'data1': out1, 'headers1': headers1,'data2': out2, 'headers2': headers2,
             'dict' : dicti,
             'name': 'INSIGHTS', 'download1': f'media/data_lineage/{nm1}','download2': f'media/data_lineage/{nm2}'})
