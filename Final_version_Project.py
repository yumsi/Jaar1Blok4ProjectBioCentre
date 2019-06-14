########################################################################
# Author:Han Teunissen and Harm Laurense
# last update: 13-6-2019
# Function: Runs the project webaplication
# Known bugs: Sometimes does not render the header in the HTML documnents
# correctly
########################################################################
from flask import Flask, render_template, request
import mysql.connector


app = Flask(__name__)


@app.route('/')
def index():
    """Renders the index/home HTML template"""
    return render_template('Blok4_Home.html')


@app.route('/Blok4_Blast.html')
def result():
    """Renders the blast HTML template"""
    return render_template('Blok4_Blast.html')


@app.route('/Blok4_Home.html')
def home():
    """Renders the home HTML template"""
    return render_template('Blok4_Home.html')


@app.route('/Blok4_About.html')
def about():
    """Renders the about HTML template"""
    return render_template('Blok4_About.html')


@app.route('/Blok4_Help.html')
def help_page():
    """Renders the help HTML template"""
    return render_template('Blok4_Help.html')


@app.route('/Blok4_Count_Org.html')
def count_render():
    """Renders the Count HTML template"""
    return render_template('Blok4_Count_Org.html')


@app.route('/invoer', methods=['POST', 'GET'])
def blast():
    """Gets the string needed for a query through the selector function and
    gives it to the get_query function where it gets the requested results
    and than renders the blast template HTML template with the result from
    the get_query function"""
    q_string, column = selector()
    rows = get_query(q_string)
    return render_template('Blok4_Blast.html', resultaat=rows, column=column)


def selector():
    """The selector function gets the user input of which boxes where
    checked. It confirms this by checking wether by checking if the value of
    the box equals to a sting. Futhermore it also reutrns a list of the
    selected choices and a list for the head columns"""
    q_string = ''
    column = []
    select = []
    # Gets the users choices
    acc_code = request.form.get('ACS')
    e_val = request.form.get('EVAL')
    prot = request.form.get('PROT')
    organisme = request.form.get('ORG')
    alignment_score = request.form.get('ALS')
    ident_per = request.form.get('IDEN')
    q_cover = request.form.get('QCOV')
    fragment = request.form.get('FRAG')
    fragmentseq = request.form.get('FRAGSEQ')
    eiwitseq = request.form.get('SEQ')
    taxonomie = request.form.get('TAX')
    acc_code_org = request.form.get('ACSORG')
    desc = request.form.get('DESC')
    select.append(fragment)
    select.append(organisme)
    select.append(taxonomie)
    select.append(acc_code_org)
    select.append(prot)
    select.append(acc_code)
    select.append(e_val)
    select.append(alignment_score)
    select.append(ident_per)
    select.append(q_cover)
    select.append(desc)
    select.append(eiwitseq)
    select.append(fragmentseq)
    # checks what the user has chosen and makes one string of it
    for item in select:
        if type(item) == str:
            column.append(item)
            q_string = q_string + item + ', '
    q_string = q_string[:-2]
    return q_string, column


def get_query(q_string):
    """The get_query function connects to the database, gets the search term
     from the user and than processes the query using mysql.connector.
     Finally the funcitons returns the results from the query as rows """
    conn = mysql.connector.connect(
        host="hannl-hlo-bioinformatica-mysqlsrv.mysql.database.azure.com",
        user="yumsi@hannl-hlo-bioinformatica-mysqlsrv",
        password="yumsi11",
        db="yumsi")
    name = request.form['zoekwoord']
    n = request.form['limit']
    if n == '':
        # Input result limit, no input results in the standard use of limit 100
        n = 100
    else:
        n = int(n)
    if name != "" and q_string != "":
        cursor = conn.cursor()
        # Query for the database
        query = """SELECT  """ + q_string + """
                FROM ProjectBlok4_Protein
                JOIN ProjectBlok4_Lineage ON ProjectBlok4_Protein.Eiwit_ID=ProjectBlok4_Lineage.Lineage_ID
                JOIN ProjectBlok4_Fragment ON ProjectBlok4_Protein.Eiwit_ID=ProjectBlok4_Fragment.Fragment_ID
                JOIN ProjectBlok4_Function ON ProjectBlok4_Protein.Eiwit_ID=ProjectBlok4_Function.Functie_ID
                JOIN ProjectBlok4_Taxonomy ON ProjectBlok4_Protein.Eiwit_ID=ProjectBlok4_Taxonomy.Taxonomie_ID
    WHERE ProjectBlok4_Protein.Accessiecode LIKE '%""" + name + """%'or  
      ProjectBlok4_Protein.Alignment_scores LIKE '%""" + name + """%' or
      ProjectBlok4_Protein.Eiwit_Naam LIKE '%""" + name + """%' or 
      ProjectBlok4_Protein.Expect LIKE '%""" + name + """%' or
      ProjectBlok4_Protein.Per_ident LIKE '%""" + name + """%' or 
      ProjectBlok4_Protein.Query_coverage LIKE '%""" + name + """%' or
      ProjectBlok4_Protein.Sequentie LIKE '%""" + name + """%' or
      ProjectBlok4_Fragment.Fragment_naam LIKE '%""" + name + """%' or 
      ProjectBlok4_Function.Functie_naam LIKE '%""" + name + """%'or  
      ProjectBlok4_Taxonomy.Taxonomie LIKE '%""" + name + """%' or 
      ProjectBlok4_Taxonomy.Accessiecode_taxonomie LIKE '%""" + name + """%'
            LIMIT {0};"""
        cursor.execute(query.format(n))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    rows = ""
    return rows


@app.route('/count_org', methods=['POST', 'GET'])
def count():
    """The count connects to the database and gets the searchterm from the
    user. Furthermore it calls the select_count funcctions wich returns and
    then calls the appropriate functon based on the user decisions to get
    the results. Finally the functions renders the Count HTML template with
    the results"""
    conn = mysql.connector.connect(
        host="hannl-hlo-bioinformatica-mysqlsrv.mysql.database.azure.com",
        user="yumsi@hannl-hlo-bioinformatica-mysqlsrv",
        password="yumsi11",
        db="yumsi")
    zoekwoord = request.form['zoekwoord']
    select = select_count()
    # checks wich choice the user has made
    if select == 'Eiwitten':
        count_result = count_eiwit_getquery(conn, zoekwoord)
        column = ['Naam van eiwit', 'Totaal']
    elif select == 'Organismen':
        count_result = count_org_getquery(conn, zoekwoord)
        column = ['Naam van organismen', 'Totaal']
    return render_template('Blok4_Count_Org.html', resultaat=count_result,
                           column=column)


def select_count():
    """The select_count function gets wich radiobutton the user has selected
    and returns this choice as select"""
    select = request.form.get('choice')
    return select


def count_org_getquery(conn, zoekwoord):
    """ The count_org_getquery function cconnects to the database calls the
    functionrow_sorter. And than
    processes the query using mysql.connector.
     Finally the funcitons returns the results from the query as
     count_results
     :param conn: connection info to th edatabase
     :param zoekwoord: search term from the user"""
    cursor = conn.cursor()
    # database query
    query = """select Lineage_naam
from ProjectBlok4_Lineage join ProjectBlok4_Fragment
where Lineage_ID = Fragment_ID and Lineage_naam like '%""" + zoekwoord + """%'
group by Fragment_naam
;"""
    cursor.execute(query)
    rows = cursor.fetchall()
    count_result = row_sorter(rows)
    return count_result


def count_eiwit_getquery(conn, zoekwoord):
    """ The count_eiwit_getquery function cconnects to the database calls the
    function row_sorter. And than
    processes the query using mysql.connector.
     Finally the funcitons returns the results from the query as
     count_results
     :param conn: connection info to th edatabase
     :param zoekwoord: search term from the user"""
    cursor = conn.cursor()
    # database query
    query = """select Eiwit_Naam
from projectblok4_protein join projectblok4_fragment
where Eiwit_ID = Fragment_ID and Eiwit_Naam like '%""" + zoekwoord + """%'
group by Fragment_naam
;"""
    cursor.execute(query)
    rows = cursor.fetchall()
    count_result = row_sorter(rows)
    return count_result


def row_sorter(rows):
    """The row_sorter function sorts the rows from the rows parameter and
    counts how many unique rows there are in the variable rows. And then
    returns it a single tuple list
    :param rows: tuples from the query
    :return: show_ls: list of tuples wich contains the amount of how many
    times an unique row was in rows
    """
    show_ls = []
    count_ls = []
    distinct_ls = []
    count_row = 0
    for row in rows:
        if row not in distinct_ls:
            distinct_ls.append(row)
            count_ls.append(1)
        else:
            # counts the how many times a row is in rows
            dis_pos = distinct_ls.index(row)
            count_ls[dis_pos] = int(count_ls[dis_pos]) + 1
    for item in distinct_ls:
        amount = str(count_ls[count_row])
        # adds an variable to the tuple item
        show = item + (amount,)
        show_ls.append(show)
        count_row += 1
    return show_ls


if __name__ == '__main__':
    app.run()
