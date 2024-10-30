import streamlit as st
from elasticsearch import Elasticsearch
from util.ingest import ingest_to_elasticsearch,ingestAsTxt
import pandas as pd
import os
import json
from elasticsearch import Elasticsearch, helpers
import time

st.title("Under Development 🚧")

# # Initialize Elasticsearch
# es = Elasticsearch([{'host': 'localhost', 'port': 9200,'scheme':'http'}])


# # Streamlit app title
# tab1, tab2 , tab3, tab4= st.tabs(["Search 🔍","Input data","List Index","Documentation"])

# with tab1:
#     st.title("DataBreach Search 🔍")

#     # Search bar
#     with st.container(border=True):
#         query = st.text_input("Enter search string:")
#         col1, col2, col3 = st.columns([1, 1, 4])
#         with col1:
#             submit = st.button("Submit",type="primary",use_container_width=True)
#         with col2:
#             help = st.button("Help",use_container_width=True)
#     # Function to format index names
#     def format_index_name(index_name):
#         index_name = index_name.replace('-', '/')
#         index_name = index_name.replace('-', '/')
#         return index_name.replace('i../', '')

#     # Query Elasticsearch when the user inputs a search string
#     if submit:
#         search_body = {
#             "query": {
#                 "query_string": {
#                     "query": query
#                 }
#             }
#         }

#         # Perform the search across all indices
#         with st.status("Searching...") as status:
#             start_time = time.time()
#             response = es.search(index="_all", body=search_body, size=1000)
#             end_time = time.time()
#             total_time = end_time - start_time
#             status.update(
#                 label=f"Search complete! (Total time: {total_time:.2f} seconds)", state="complete", expanded=False
#             )

#         if response['hits']['total']['value'] == 0:
#             st.error("### No Record Found !")
#         else:
#             # Process and display the results
#             hits_by_index = {}
#             for hit in response['hits']['hits']:
#                 index = hit['_index']
#                 if index not in hits_by_index:
#                     hits_by_index[index] = []
#                 hits_by_index[index].append(hit['_source'])

#             for index, items in hits_by_index.items():
#                 formatted_index = format_index_name(index)
#                 st.write(f"Found {len(items)} item(s) on: {formatted_index}")
#                 df = pd.DataFrame(items)
#                 st.dataframe(df,use_container_width=True)

#     if help:
#         st.info("""### Daftar Query :\n
# ##### Query : *target*\n
#             Digunakan Untuk mencari record yang mengandung kata 'target' (exact match)  \n
#     Contoh hasil yang sesuai : 'Target satu' , 'Dalam Target'\n
# ##### Query : \*target\*  (diapit oleh *asterisk*)\n
#             Digunakan Untuk mencari record yang mengandung kata 'target' \n
#     Contoh hasil yang sesuai : 'Target1 Satu' , '2target Dua'\n
# ##### Query : target*\n
#             Sigunakan untuk mencari record yang diawali dengan 'target'\n
#     Contoh hasil yang sesuai : '1Target','Satu Target'""")


# with tab2:
#     st.title("Elasticsearch Data Ingestion App")
#     input_path = st.text_input("EnterInput:")
#     submit_input = st.button("Input",type="primary",use_container_width=True)
#     submit_as_txt = st.button("Input as raw txt",use_container_width=True)
#     help2 = st.button("Bantuan",use_container_width=True)

#     if submit_as_txt:
#         try:
#             with st.status("Ingesting") as status:
#                 ingestAsTxt(input_path)
#                 status.update(label="Completed",state="complete")
#             st.success(f"Successfully ingested {input_path} into Elasticsearch.")
#         except Exception as e:
#             st.error(f"An error occurred: {str(e)}")
#     # Button to trigger ingestion
#     if submit_input:
#         # Ingest data to Elasticsearch
#         try:
#             with st.status("Ingesting") as status:
#                 total_doc = ingest_to_elasticsearch(input_path)
#                 status.update(label="Completed",state="complete")
#             st.success(f"Successfully ingested {total_doc} of file {input_path} into Elasticsearch.")
#         except Exception as e:
#             st.error(f"An error occurred: {str(e)}")
#             st.warning("Jika Gagal, dapat merubah file extension menjadi txt")
    
#     if help2:
#         st.info("""## Bantuan\n
# ### Input
# Jenis input yang dimasukan adalah `PATH` dari file. Ekstensi file yang disupport : `csv, tsv, json, txt`\n
# Contoh: \n
#         C:/data/data.txt\n
#     /root/root/data.txt\n
# ### Error Input
# Jika terjadi error saat pengimputan data, bisa jadi diakibatkan oleh kesalahan formatting pada data. Untuk Alternatif dapat menggunakan tombol\n 
#         Input as raw text""")

# with tab3:
#     def get_indices():
#         # Get all indices
#         indices = es.indices.get_alias(index="*")
#         # Filter out indices that start with a dot
#         return [index for index in indices if not index.startswith(".")]

#     def get_index_stats(index):
#         # Get stats for a specific index
#         stats = es.indices.stats(index=index)
#         num_docs = stats['indices'][index]['total']['docs']['count']
#         size_in_bytes = stats['indices'][index]['total']['store']['size_in_bytes']
#         size_in_gb = size_in_bytes / (1024 ** 3)  # Convert to GB
#         return num_docs, size_in_gb

#     def delete_indices(selected_indices):
#         for index in selected_indices:
#             es.indices.delete(index=index)

#     # Streamlit app title
#     st.title("Elasticsearch Index Management")

#     # Get all available indices
#     indices = get_indices()

#     # If there are no indices, display a message
#     def check_indices():
#         if not indices:
#             st.write("No indices available.")
#         else:
#             # Create a form with checkboxes for each index
#             with st.form(key='delete_form'):
#                 st.write("Select indices to delete:")
#                 selected_indices = []
                
#                 for index in indices:
#                     num_docs, size_in_gb = get_index_stats(index)
#                     label = f"{index} (Documents: {num_docs}, Size: {size_in_gb:.2f} GB)"
#                     if st.checkbox(label):
#                         selected_indices.append(index)
                    
#                 # Add a submit button to the form
#                 delete_button = st.form_submit_button(label='Delete Selected Indices')
                
#                 if delete_button:
#                     if selected_indices:
#                         delete_indices(selected_indices)
#                         st.success(f"Successfully deleted indices: {', '.join(selected_indices)}")
#                         st.rerun()
#                     else:
#                         st.warning("No indices selected for deletion.")

#     check_indices()

# with tab4:
#     st.write()

