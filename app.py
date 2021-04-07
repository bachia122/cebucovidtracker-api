from flask import request, jsonify, Flask
import sqlite3
from flask_cors import CORS
from datetime import datetime, timedelta
from os import listdir
import requests
import pandas as pd 
import json


app = Flask(__name__)
CORS(app)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})
app.config["DEBUG"] = True

db_date = (datetime.utcnow() - timedelta(hours=13)).strftime("%Y-%m-%d")
conn = sqlite3.connect(db_date + '_beds.db')
qbdate = 'SELECT MAX(reportdate) as date_reported FROM Beds'
conn = sqlite3.connect(db_date + '_tests.db')
qtdate = 'SELECT MAX(report_date) as date_reported FROM Tests' 

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

@app.route('/test', methods=['GET'] )
def test():
    return 'test purposes only'

@app.route('/test2', methods=['GET'] )
def test2():
    conn = sqlite3.connect(db_date + '_cases.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    new = cur.execute('SELECT COUNT(*) as new FROM Cases GROUP BY DateRepConf;').fetchall()
    return jsonify(new)



@app.route('/', methods=['GET'])
def home():
    return '''<h1>CEBU COVID19 TRACKER</h1>
    <p>A prototype API for tracking COVID19 in Cebu. Data updated every 8PM PHT.</p>
    <p> How to use: <p>
    /api/dates  -----------------> times and dates of source & update

    <p> RAW DATA <br>
    /api/cases/allcases --------------------> all recorded cases <br>
    /api/cases?status=ACTIVE ----------> all active cases <br>
    /api/cases?status=RECOV -------> all recovered cases <br>
    /api/cases?status=DIED ---------> all deaths <br>
    /api/tests/alltests --------------------> all testing records per testing facility<br>
    /api/beds/allbeds --------------------> all bed records per COVID19 facility<br>

    <p> CASE STATS <br>
    /api/cases/totals -----------------> cumulative tallies incl. new cases, etc <br>
    /api/cases/casebycitymun --------------------> # of active cases by city or municipality <br>
    /api/cases/top5 --------------------> top 5 cities/municipalities by active cases <br>
    /api/cases/symptoms --------------> number of admitted COVID19 patients grouped by degree of symptoms <br>

    <p> TIME SERIES OF CASES <br>
    /api/cases/charts?status=NEW ---------> count of NEW cases as of source date <br>
    /api/cases/charts?status=RECOV ---------> daily recoveries // NOTE: thousands of recoveries not officially recorded // <br>
    /api/cases/charts?status=DIED ---------> daily deaths<br>
    7 DAY AVERAGE:<br>
    /api/cases/charts/average ---> 7 day average of new cases (others in progress)

    <p> TESTING STATS <br> 
    /api/tests/testoverview --------------> tallies incl. test counts, positivity rate, remaining tests, etc

    <p> BED OCCUPANCY STATS<br>
    /api/beds/bedoverview --------------> tallies incl. number of vacant/occupied ICU/non-ICU beds, occupancy rates, etc <br> 
    /api/beds/crit_faci --------------> facilities with critical bed occupancy (>=85%) <br> 
    /api/beds/severe_faci --------------> facilities with severe bed occupancy (>=70%) <br> 
    /api/beds/high_faci --------------> facilities with high bed occupancy (>=60%)

    <p> LATEST NEWS <br> 
    /api/news --------------------> first 5 articles from Google News search keywords 'Cebu covid"
    <p> 

    <button onclick="location.href='mailto:cebucovidtracker@gmail.com';"> report an issue</button>

    '''


@app.route('/api/dates', methods=['GET'])
def api_date():
    dates = {'source_date': db_date, 'update_frequency': 'Every 9PM PHT'}
    return jsonify(dates)

#######################
# TESTING INFORMATION #
#######################

@app.route('/api/tests/alltests', methods=['GET'])
def api_alltests():
    conn = sqlite3.connect(db_date + '_tests.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    all_tests = cur.execute('SELECT * FROM Tests;').fetchall()
    return jsonify(all_tests)


@app.route('/api/tests/testoverview', methods=['GET'] )
def api_test_overview():
    conn = sqlite3.connect(db_date + '_tests.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    date_reported = cur.execute(qtdate).fetchall()

    q = ' FROM Tests WHERE report_date=(' + qtdate +');'
    total_indv = cur.execute('SELECT SUM(cumulative_unique_individuals) as total_indv' + q).fetchall()
    total_pos = cur.execute('SELECT SUM(cumulative_positive_individuals) as total_pos ' + q).fetchall()
    total_neg = cur.execute('SELECT SUM(cumulative_negative_individuals) as total_neg ' + q).fetchall()

    new_indv = cur.execute('SELECT SUM(daily_output_unique_individuals) as new_indv' + q).fetchall()
    new_pos_indv = cur.execute('SELECT SUM(daily_output_positive_individuals) as new_pos_indv' + q).fetchall()
    pct_pos = cur.execute('SELECT ROUND(100*SUM(daily_output_positive_individuals)/SUM(daily_output_unique_individuals),1) as pct_pos' + q).fetchall()
    remaining_tests = cur.execute('SELECT SUM(remaining_available_tests) as remaining_tests' + q).fetchall()
    test_labs = cur.execute('SELECT COUNT(*) as test_labs' + q).fetchall()
    
    test_overview = date_reported + total_indv + total_pos + total_neg + new_indv + new_pos_indv + pct_pos + remaining_tests + test_labs 
    return jsonify(test_overview)


####################
# BED AVAILABILITY #
####################
@app.route('/api/beds/allbeds', methods=['GET'])
def api_allbeds():
    conn = sqlite3.connect(db_date + '_beds.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    all_beds = cur.execute('SELECT * FROM Beds;').fetchall()
    return jsonify(all_beds)

@app.route('/api/beds/bedoverview', methods=['GET'] )
def api_beds_overview():
    conn = sqlite3.connect(db_date + '_beds.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    date_reported = cur.execute(qbdate).fetchall()

    q = ' FROM Beds WHERE reportdate=('+ qbdate +');'
    num_faci = cur.execute('SELECT COUNT(*) as num_faci' + q).fetchall()
    o_icu_beds = cur.execute('SELECT SUM(icu_o) as o_icu_beds' + q).fetchall()
    v_icu_beds = cur.execute('SELECT SUM(icu_v) as v_icu_beds' + q).fetchall()
    t_icu_beds = cur.execute('SELECT SUM(icu_o + icu_v) as t_icu_beds' + q).fetchall()
    pct_icu_occ = cur.execute('SELECT ROUND(100*SUM(icu_o)/SUM(icu_o + icu_v),1) as pct_icu_occ' + q).fetchall()
    o_nonicu = cur.execute('SELECT SUM(isolbed_o + beds_ward_o) as o_nonicu' + q).fetchall()
    v_nonicu = cur.execute('SELECT SUM(isolbed_v + beds_ward_v) as v_nonicu' + q).fetchall()
    t_nonicu = cur.execute('SELECT SUM(isolbed_o + beds_ward_o + isolbed_v + beds_ward_v) as t_nonicu' + q).fetchall()
    pct_nonicu_occ = cur.execute('SELECT ROUND(100*SUM(isolbed_o + beds_ward_o)/SUM(isolbed_o + beds_ward_o + isolbed_v + beds_ward_v),1) as pct_nonicu_occ' + q).fetchall()

    beds_overview = date_reported + num_faci + o_icu_beds + v_icu_beds + t_icu_beds + pct_icu_occ + o_nonicu + v_nonicu + t_nonicu + pct_nonicu_occ
    return jsonify(beds_overview)


# occupancy >=85% :critical, >=70% :severe, >=60 %high
q_occu_pct = ' SUM(icu_o + isolbed_o + beds_ward_o)/SUM(icu_o + icu_v + isolbed_o + beds_ward_o + isolbed_v + beds_ward_v) '
qfaci = 'SELECT cfname as faci_name FROM Beds WHERE reportdate=('+ qbdate +') GROUP BY cfname HAVING ' + q_occu_pct
@app.route('/api/beds/crit_faci', methods=['GET'] )
def api_crit_facilities():
    conn = sqlite3.connect(db_date + '_beds.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    crit_faci = cur.execute(qfaci + '>=0.85;').fetchall()
    return jsonify(crit_faci)
@app.route('/api/beds/severe_faci', methods=['GET'] )
def api_severe_facilities():
    conn = sqlite3.connect(db_date + '_beds.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    severe_faci = cur.execute(qfaci + '>= 0.7 AND '+ q_occu_pct + ' <0.85;').fetchall()
    return jsonify(severe_faci)
@app.route('/api/beds/high_faci', methods=['GET'] )
def api_high_facilities():
    conn = sqlite3.connect(db_date + '_beds.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    high_faci = cur.execute(qfaci + '>= 0.6 AND '+ q_occu_pct + ' <0.7;').fetchall()
    return jsonify(high_faci)

####################
# CASE INFORMATION #
####################

@app.route('/api/cases/allcases', methods=['GET'])
def api_allcases():
    conn = sqlite3.connect(db_date + '_cases.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    all_cases = cur.execute('SELECT * FROM Cases;').fetchall()
    return jsonify(all_cases)

@app.route('/api/cases/totals', methods=['GET'])
def api_countcases():
    conn = sqlite3.connect(db_date + '_cases.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    total = cur.execute('SELECT COUNT(*) as total FROM Cases;').fetchall()
    active = cur.execute('SELECT COUNT(DateRepConf) as active FROM Cases WHERE NOT HealthStatus = "RECOVERED" AND NOT HealthStatus = "DIED";').fetchall()
    new_active = cur.execute("SELECT COUNT(*) as new_today FROM Cases WHERE DateRepConf=:date;", {"date":db_date}).fetchall()
    recov = cur.execute('SELECT COUNT(*) AS recoveries FROM Cases WHERE DateDied IS "NULL" AND HealthStatus = "RECOVERED";').fetchall()
    new_recov = cur.execute("SELECT COUNT(*) as recov_today FROM Cases WHERE DateRecover = :date ;", {"date":db_date}).fetchall()
    died = cur.execute('SELECT COUNT(DateDied) as deaths FROM Cases WHERE DateDied IS NOT "NULL";').fetchall()
    new_died = cur.execute('SELECT COUNT(*) as died_today FROM Cases WHERE DateDied= :date ;', {'date':db_date}).fetchall()
    total_counts =  total + active + new_active + recov + new_recov + died + new_died
    return jsonify(total_counts)

@app.route('/api/cases/casebycitymun', methods=['GET'] )
def api_cases_by_citymuni():
    conn = sqlite3.connect(db_date + '_cases.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    conn.create_function("power", 1, lambda x: x**0.5)
    city_muni = cur.execute('SELECT CityMunRes, COUNT(*) as local_cases, power(COUNT(*)) as size FROM Cases WHERE NOT HealthStatus = "RECOVERED" AND NOT HealthStatus = "DIED" GROUP BY CityMunRes;').fetchall()
    coord = pd.read_csv('citymun_coord.csv') #
    citymun_w_coord = []
    for d1 in city_muni: 
        for d2 in coord.to_dict('r'):
            if d1['CityMunRes'] == d2['CityMunRes']:
                citymun_w_coord.append({**d1, **d2})
    return jsonify(citymun_w_coord)

@app.route('/api/cases/top5', methods=['GET'] )
def top5():
    conn = sqlite3.connect(db_date + '_cases.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    top_5 = cur.execute('SELECT CityMunRes as top_5, COUNT(*) as cases FROM Cases WHERE NOT HealthStatus = "RECOVERED" AND NOT HealthStatus = "DIED" GROUP BY CityMunRes ORDER BY cases DESC LIMIT 5;').fetchall()
    return jsonify(top_5)


@app.route('/api/cases/symptoms', methods=['GET'] )
def api_symptoms():
    conn = sqlite3.connect(db_date + '_beds.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    date_reported = cur.execute(qbdate).fetchall()

    q = ' FROM Beds WHERE reportdate=('+ qbdate +');'
    num_asym = cur.execute('SELECT SUM(conf_asym) as num_asym' + q).fetchall()
    num_mild = cur.execute('SELECT SUM(conf_mild) as num_mild' + q).fetchall()
    num_sev = cur.execute('SELECT SUM(conf_severe) as num_severe' + q).fetchall()
    num_crit = cur.execute('SELECT SUM(conf_crit) as num_crit' + q).fetchall()

    symptoms_overview = date_reported + num_asym +  num_mild + num_sev + num_crit
    return jsonify(symptoms_overview)


@app.route('/api/cases/charts', methods=['GET'])
def api_chart():
    query_parameters = request.args
    status = query_parameters.get('status')
    if status == "NEW":
        query = 'SELECT DateNewCase, COUNT(*) as new_cases FROM Cases WHERE DateNewCase IS NOT "NULL" GROUP BY DateNewCase;'
        dateType = 'DateNewCase'
        caseType = 'new_cases'
    if status == "ACTIVE":
        query = 'SELECT DateRepConf, COUNT(*) as active_cases FROM Cases WHERE NOT HealthStatus = "RECOVERED" AND NOT HealthStatus = "DIED" GROUP BY DateRepConf;'
        dateType = 'DateRepConf'
        caseType = 'active_cases'
    if status == "RECOV":
        query = 'SELECT DateRecover, COUNT(*) as recoveries FROM Cases WHERE DateRecover IS NOT "NULL" and HealthStatus = "RECOVERED" GROUP BY DateRecover;'
        dateType = 'DateRecover'
        caseType = 'recoveries'
    if status == "DIED":
        query = 'SELECT DateDied, COUNT(*) as deaths FROM Cases WHERE DateDied IS NOT "NULL" GROUP BY DateDied ORDER BY DateDied ASC;'
        dateType = 'DateDied'
        caseType = 'deaths'
    conn = sqlite3.connect(db_date + '_cases.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()
    results = cur.execute(query).fetchall()
    start = datetime.strptime("2020-03-01", "%Y-%m-%d")
    dataset = []
    for result in results:
        date = datetime.strptime(result[dateType], "%Y-%m-%d")
        while start < date:      
            dataset.append( {dateType: start.strftime("%Y-%m-%d"), caseType: 0} )
            start += timedelta(days=1)
        start += timedelta(days=1)
        dataset.append(result)
    return jsonify(dataset)

### 7 day average for new cases only
@app.route('/api/cases/charts/average', methods=['GET'] )
def average_7day():
    url = 'https://testflask122.herokuapp.com/api/cases/charts?status=NEW'
    r = requests.get(url)
    data = r.json()
    ave = [{'DateAveNew': data[i].get('DateNewCase'), 'new_ave': 0 } for i in range(6)]
    for i in range(6,len(data)):
        date = data[i].get('DateNewCase')
        c = 0
        for j in range(7):
            cases = data[i-j].get('new_cases')
            c += cases
        count = c/7
        ave.append( {'DateAveNew': date, 'new_ave': float('{:.1f}'.format(count)) } )
    return jsonify(ave)

@app.route('/api/cases', methods=['GET'])
def api_filter():
    query_parameters = request.args
    status = query_parameters.get('status')

    query = "SELECT * FROM Cases WHERE"
    #to_filter = []

    if status == "ACTIVE":
        query += ' NOT HealthStatus = "RECOVERED" AND NOT HealthStatus = "DIED";'
        #to_filter.append(status)
    if status == "DIED":
        query += ' DateDied IS NOT "NULL";'
        #to_filter.append(status)
    if status == "RECOV":
        query += ' DateDied IS "NULL" AND HealthStatus = "RECOVERED";'
        #to_filter.append(status)
    #if not (HealthStatus or published or author):
        #return page_not_found(404)

    #query = query[:-4] + ';'

    conn = sqlite3.connect(db_date + '_cases.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()

    results = cur.execute(query).fetchall()

    return jsonify(results)


@app.route('/api/news', methods=['GET'] )
def api_news():
    with open('articles.txt') as json_file:
        data = json.load(json_file)
    return jsonify(data)



@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404
