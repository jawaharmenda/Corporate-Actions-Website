from __future__ import print_function
import flask
import pyodbc
import sys
import pandas as pd
import time
from flask import Flask,render_template,request,redirect,flash,url_for

app=Flask(__name__)
app.secret_key = 'MJR'

def EDI_cnxn():
	edicnxn = pyodbc.connect("Driver={SQL Server};Server=ii02-b4wyvq1\SQLEXPRESS;Database=EDICA;UID=test;PWD=test123")
	return edicnxn

EDIcnxn=EDI_cnxn()

def SNL_cnxn():
	snlcnxn=pyodbc.connect("Driver={SQL Server};Server=ii02-b4wyvq1\SQLEXPRESS;Database=SNLCA;UID=test;PWD=test123")
	return snlcnxn

SNLcnxn=SNL_cnxn()

def Index_cnxn():
	indexcnxn=pyodbc.connect("DRIVER={SQL Server};SERVER=ii02-b4wyvq1\SQLEXPRESS;Database=IndexCA;UID=test;PWD=test123")
	return indexcnxn

Indexcnxn=Index_cnxn()

@app.route('/')
def Index():
	return render_template('CorporateActions.html')

@app.route('/vendor',methods=['GET','POST'])
def vendor():
	if request.method=='POST':
		symbol1=request.form['CAL']
		symbol2=request.form['calendarFrom']
		symbol3=request.form['calendarTo']
		symbol4=request.form['companyname']
		if not symbol2 and symbol3 and symbol4:
			flash('testing')
			return redirect(url_for('Index'))
		if symbol1=='EDI':
			print(symbol1,file=sys.stderr)
			query1 = """select distinct edift.eventcd,edift.eventid,edift.issuername,edift.mic,excg3.country as EDI_Country,excg3.exchangename as EDI_ExchnageName,edift.isin as WCA680FULL_ISIN,edicp.isin as WCA680_CUSIP_ISIN,edicp.CUSIP from EDICA..WCA680Full_tbl(nolock) edift join EDICA..WCA680_CUSIP_tbl(nolock) edicp on edift.isin = edicp.isin join SNLCA..Exchange_tbl(nolock) excg3 on excg3.MIC = edift.mic where CONVERT(date,edift.FileDate) between '{}' and '{}'""".format(symbol2,symbol3)
			data = pd.read_sql(query1,EDIcnxn)
			data = data.to_html()
		elif symbol1=='IDC':
			print(symbol1,file=sys.stderr)
			query2="""select distinct ft.CUSIP,ft.ExDate,ft.PayDate,ft.RecordDate,ft.Rate,ft.nasdaqCode,convert(date,ft.filedate) as FileDate,ref.exchangeid, map.CIQExchangeName,map.CIQExchangeID from SNLCA..IDCCAFull_tbl(nolock)ft join SNLCA..RefData_ExchangeCodeDomestic_tbl(nolock) ref on ft.ExchangeCode = ref.exchangeCode join SNLCA..SNL_ExchangeMapping_tbl(nolock) map on ref.exchangeId = map.CIQExchangeID where CONVERT(date, ft.filedate) between '{}' and '{}'""".format(symbol2,symbol3)
			data = pd.read_sql(query2,SNLcnxn)
			data = data.to_html()
		elif symbol1=='Index':
			print(symbol1,file=sys.stderr)
			query3="""select sptsx.INDEXNAME,sptsx.INDEXCODE,sptsx.INDEXKEY,sptsx.INDEXCURRENCY,sptsx.ACTIONTYPE,sptsx.ACTIONGROUP,sptsx.EFFECTIVEDATE,sptsx.STOCKKEY,
					sptsx.CURRENTCOMPANYNAME,sptsx.CURRENTCUSIP,sptsx.CURRENTISIN,sptsx.CURRENTSEDOL,sptsx.CURRENTTICKER,sptsx.NEWTICKER,sptsx.CURRENTMIC,convert(date,sptsx.FileDate) as FileDate
					from IndexCA..SPTSX_Master(nolock) sptsx
					union
					select sptmi.INDEXNAME,sptmi.INDEXCODE,sptmi.INDEXKEY,sptmi.INDEXCURRENCY,sptmi.ACTIONTYPE,sptmi.ACTIONGROUP,sptmi.EFFECTIVEDATE,sptmi.STOCKKEY,sptmi.CURRENTCOMPANYNAME,sptmi.CURRENTCUSIP,
					sptmi.CURRENTISIN,sptmi.CURRENTSEDOL,sptmi.CURRENTTICKER,sptmi.NEWTICKER,sptmi.CURRENTMIC,convert(date,sptmi.FileDate) as FileDate 
					from IndexCA..Daily_SP_Events_tbl(nolock) sptmi
					where CONVERT(date,sptmi.FileDate) between '{}' and '{}'""".format(symbol2,symbol3)
			query3=query3.replace('\n',' ')
			data = pd.read_sql(query3,Indexcnxn)
			data = data.to_html()
		elif symbol4 and not symbol1 and not symbol2 and not symbol3:
			print(symbol4,file=sys.stderr)
			query4=	"""select distinct top 10 edift.eventcd,edift.eventid,edift.isin AS EDI_ISIN, edift.issuername,edift.securitydesc,edift.parvalue,edift.mic as EDI_MIC,edift.field1 AS EDI_CorpAction,excg.exchangename AS EDI_Exchange, excg.country AS EDI_Country,
					sptsx.INDEXNAME as SPTSX_INDEXNAME ,sptsx.INDEXCODE as SPTSX_INDEXCODE,sptsx.INDEXKEY as SPTSX_INDEXKEY,sptsx.INDEXCURRENCY as SPTSX_INDEXCURRENCY,sptsx.ACTIONTYPE as SPTSX_ACTIONTYPE,
					sptsx.ACTIONGROUP as SPTSX_ACTIONGROUP,
					sptsx.EFFECTIVEDATE as SPTSX_EFFECTIVEDATE,
					sptsx.STOCKKEY as SPTSX_STOCKKEY,sptsx.CURRENTCOMPANYNAME as SPTSX_CURRENTCOMPANYNAME,sptsx.CURRENTCUSIP as SPTSX_CURRENTCUSIP,sptsx.CURRENTISIN as SPTSX_CURRENTISIN,
					sptsx.CURRENTSEDOL as SPTSX_CURRENTSEDOL,sptsx.CURRENTTICKER as SPTSX_CURRENTTICKER,sptsx.NEWTICKER as SPTSX_NEWTICKER,sptsx.CURRENTMIC as SPTSX_CURRENTMIC,
					excg1.exchangename as SPTSX_ExchangeName,excg1.country as SPTSX_Country,
					sptmi.INDEXNAME as SPTMI_INDEXNAME ,sptmi.INDEXCODE as SPTMI_INDEXCODE,sptmi.INDEXKEY as SPTMI_INDEXKEY,sptmi.INDEXCURRENCY as SPTMI_INDEXCURRENCY,
					sptmi.ACTIONTYPE as SPTMI_ACTIONTYPE,sptmi.ACTIONGROUP as SPTMI_ACTIONGROUP,sptmi.EFFECTIVEDATE as SPTMI_EFFECTIVEDATE,
					sptmi.STOCKKEY as SPTMI_STOCKKEY,sptmi.CURRENTCOMPANYNAME as SPTMI_CURRENTCOMPANYNAME,sptmi.CURRENTCUSIP as SPTMI_CURRENTCUSIP,sptmi.CURRENTISIN as SPTMI_CURRENTISIN,
					sptmi.CURRENTSEDOL as SPTMI_CURRENTSEDOL,sptmi.CURRENTTICKER as SPTMI_CURRENTTICKER,sptmi.NEWTICKER as SPTMI_NEWTICKER,sptmi.CURRENTMIC as SPTMI_CURRENTMIC,
					excg2.exchangename as SPTMI_ExchangeName,excg2.country as SPTMI_Country
					from IndexCA..SPTSX_Master(nolock) sptsx
					join IndexCA..Daily_SP_Events_tbl(nolock) sptmi on sptsx.CURRENTISIN = sptmi.CURRENTISIN
					join EDICA..WCA680Full_tbl(nolock) edift on edift.isin = sptmi.CURRENTISIN
					join SNLCA..Exchange_tbl(nolock) excg1 on excg1.MIC = sptsx.CURRENTMIC
					join SNLCA..Exchange_tbl(nolock) excg2 on excg2.MIC = sptmi.CURRENTMIC
					join SNLCA..Exchange_tbl(nolock) excg on excg.MIC = edift.mic
					where edift.issuername like '%{}%'""".format(symbol4)
			query4=query4.replace('\n',' ')
			query4=query4.replace('\t',' ')
			data = pd.read_sql(query4,EDIcnxn)
			data = data.to_html()
		return render_template('CorporateActions.html',data=data)
	else:
		return render_template('CorporateActions.html')
if __name__=="__main__":
	app.run(debug=True)