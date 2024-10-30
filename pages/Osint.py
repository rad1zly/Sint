import streamlit as st
import json
import os
import requests

def main():

    try:
        with open(".secret/api_key","r") as f:
            API_KEY = f.readline()
    except:
        st.error("Cant Fetch API Key on PWD/.secret/api_key")

    # @st.dialog("Input API Key")
    # def input_apikey():
    #     with st.form("Api_key_form"):
    #         api_key_input = st.text_input("Enter API Key", placeholder="")
    #         api_key_submit_button = st.form_submit_button("Submit",use_container_width=True)
        
    #     if api_key_input and api_key_submit_button:
    #         with open(".secret/api_key","r") as f:
    #             f.write(api_key_input)
    #             st.rerun()
    
    st.image("banner.png")
    st.markdown("<h1 style='text-align: center;'>D22 OSINT Tool</h1>", unsafe_allow_html=True)


    with st.form("osint_form"):
        request_input = st.text_input("Enter Request", placeholder="e.g. email address, username, etc.")
        submit_button = st.form_submit_button("Submit",use_container_width=True)
        # input_new_api_key_button = st.button("Input New API Key",use_container_width=True)
    
    with st.expander("Help"):
        st.markdown("Gunakan comma untuk melakukan query ke 2 target sekaligus")
        st.markdown("Contoh : `target1@mail.com,target2@gmail.com`")
        st.markdown("---")
        st.markdown("Lokasi API Key berada pada `PWD/.secret/apikey`")
    
    # if input_new_api_key_button:
    #     input_apikey()

    # Handle form submission
    if submit_button and request_input:
        results = []
        request_input = request_input.replace(",", "\n")

        # Check if history.json exists
        if os.path.exists("history.json"):
            with open("history.json", "r") as history_file:
                with st.spinner('Checking local data...'):
                    history_data = json.load(history_file)
                    # Find if the request already exists in history
                    for i in history_data:
                        if i["search"] == request_input:
                            results = i["results"]
                            st.success("Local Data Found...")

        # If no results in history, make an online request
        if not results:
            try:
                with st.spinner('Fetching data online...'):
                    url = 'https://server.leakosint.com/'
                    data = {"token": API_KEY, "request": request_input, "lang": "id"}
                    response = requests.post(url, json=data)

                    if response.status_code == 200:
                        results = response.json()
                        # Append request to each result for history
                        # if results["Status"] == "Error":
                        #     st.error(results["Error code"])
                        # with open("dummy.json", "r") as f:
                        #     results = json.load(f)
                
                        # Display the response
                        st.markdown("## Results")
                        try:
                            st.markdown(f"**Total Results Found:** {results["NumOfResults"]}")
                            st.markdown(f"**Total Database Found:** {results["NumOfDatabase"]}")
                            json_data = json.dumps(results, indent=4)
                            st.download_button(label="Download JSON", data=json_data, file_name="osint_results.json", mime="application/json")

                            for platform_name, platform_info in results["List"].items():
                                st.header(platform_name)
                                st.markdown(f"**Description:** {platform_info['InfoLeak']}")
                                for idx, entry in enumerate(platform_info["Data"]):
                                    st.subheader(f"Result {idx + 1}")
                                    for key, value in entry.items():
                                        st.markdown(f"**{key}:** {value}")
                                    st.markdown("---")
                                st.markdown("---")

                                # Append new data to history.json
                                with open("history.json", "r") as history_file:
                                    history_data = json.load(history_file)

                                history_object = {"search":request_input,
                                                "results":results}
                                history_data.append(history_object)
                                with open('history.json', 'w') as file:
                                    json.dump(history_data, file, indent=4)
                        except Exception as e:
                            st.error(e)
                            print(f"Error : {e}")
                    else:
                        st.error(f"Failed to fetch data. {response.text}")
                        print(f"Error : {response.text}")
                        return

            except Exception as e:
                st.error(e)
                print(f"Error : {e}")

if __name__ == "__main__":
    main()