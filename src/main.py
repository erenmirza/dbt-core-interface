import subprocess
import streamlit as st
import os
import re
import pandas as pd

df = pd.DataFrame(columns=['Model Name','State'])

def run_dbt_command(query_option, query_text, project_path):
    process = subprocess.Popen(
        ['pipenv', 'run', 'dbt', query_option, '--select', query_text, '--project-dir', project_path],
        stdout=subprocess.PIPE,
        universal_newlines=True
    )
    
    st.header("Latest Action")
    latest_line = st.empty()

    col1, col2, col3 = st.columns(3)
    
    col1.header('In Progress')
    col2.header('Successful')
    col3.header('Error')
    
    state_table_in_progress = col1.empty()
    state_table_sucessful = col2.empty()
    state_table_error = col3.empty()

    output = ''
    for line in iter(process.stdout.readline, ''):
        with latest_line:
            st.code(line)
            if 'START' in line:
                model_name = extract_model_name(line)
                add_model_to_df(model_name)
            if '[SUCCESS' in line:
                model_name = extract_model_name(line)
                edit_model_state(model_name, 'Successful')
            if '[ERROR' in line:
                model_name = extract_model_name(line)
                edit_model_state(model_name, 'Error')
                
        output += line
        state_table_in_progress.table(df[(df.State == 'In Progress')])
        state_table_sucessful.table(df[(df.State == 'Successful')])
        state_table_error.table(df[(df.State == 'Error')])

    process.stdout.close()
    return_code = process.wait()
    
    st.text("Full Logs")
    st.code(output)

    return return_code

def extract_model_name(text_string):
    pattern = r"\b\w+\.\w+"
    match = re.search(pattern, text_string, re.IGNORECASE)
    if match:
        model_name = match.group()
        return model_name
    else:
        return ''

def add_model_to_df(model_name):
    df.loc[len(df)] = [model_name, 'In Progress']

def edit_model_state(model_name, state):
    df_model = df['Model Name'] == model_name
    df.loc[df_model, 'State'] = state

# Streamlit app
def main():
    st.set_page_config(layout="wide")
    
    st.title("DBT Pipeline Progress")
    # st.write(os.getcwd())

    col1, col2, col3 = st.columns(3)

    query_option = col1.radio(
        "Select a dbt command from below",
        ('run', 'compile', 'test'))

    query_text = col2.text_input('Enter in your search term', '+<MODEL_NAME>')
    
    project_path = col3.text_input('Enter in your full dbt project path', "<YOUR DBT FULL PROJECT PATH>")
    
    st.write("Click the button below to run dbt:")

    if st.button("Run DBT"):
        dbt_return_code = run_dbt_command(query_option, query_text, project_path)

        if dbt_return_code == 0:
            st.success("DBT run completed successfully!")
        else:
            st.error(f"DBT run failed with exit code: {dbt_return_code}")

if __name__ == "__main__":
    main()