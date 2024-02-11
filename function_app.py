import logging
import azure.functions as func
import pandas as pd 
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver import FirefoxOptions
from nltk.stem.isri import ISRIStemmer
import time
from datetime import date
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import re 
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier
import string
import pickle
import arabicstopwords.arabicstopwords as stp
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import io
from azure.storage.blob import BlobServiceClient, BlobClient

app = func.FunctionApp()

@app.schedule(schedule="0 0 4 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
        
        # Initialize a blob service client
        conn_str = "DefaultEndpointsProtocol=https;AccountName=etimadtenders;AccountKey=uYMoq3d/5ho6SxwCATYLB4vXbhnUol/NpXcscW7J2Qg6SDO8W66U5DPZiKgPM8pVz9npdAcctFLP+AStdnDHWQ==;EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client("etimadtendersdaily")


        utc_timestamp = datetime.datetime.utcnow().replace(
            tzinfo=datetime.timezone.utc).isoformat()
        logging.info('Script Execution starting')
        logging.info('Python timer trigger function ran at %s', utc_timestamp)

        # Variables
        relevant_activity_list = [
            "تقنية المعلومات",
            "الإتصالات",
            "أنشطة الأمن و السلامة",
            "أنشطة التدريب",
            "أنشطة التعليم",
            "الأنشطة الاستشارية الأخرى",
            "انشطة التأمين",
            "أنشطة زراعية",
            "انشطة الاستشارات الادارية",
            "الخدمات البيئية",
            "البترول",
            "انشطة الاستشارات المالية",
            "الخدمات الرياضية",
            "الخدمات التجارية و الإدارية",
            "أنشطة سياحية",
            "انشطة الاستشارات الأمنية",
            "المالية والتمويل",
            "انشطة الاستشارات القانونية", 
            "انشطة الاستشارات الهندسية",
            "انشطة الاستشارات المالية"
            
        ]

        # Initiating dict to store scraped contents
        scraped_data = {"date_published":[],
                        "rfp_type":[],
                        "main_activity_type":[],
                        "rfp_name":[],
                        "entity_name":[],
                        "details_link":[],
                        "days_left":[],
                        "reference_number":[],
                        "last_date_clarifications":[],
                        "last_date_submission":[],
                        "review_start":[],
                        "rfp_price":[]}

        # Current date in the desired format (e.g., "10Jan2024")
        date_string = datetime.now().strftime("%d%b%Y")

        punctuations = '''`÷×؛<>_()*&^%][ـ،/:"؟.,'{}~¦+|!”…“–ـ''' + string.punctuation

        stop_words_arabic = stp.stopwords_list()

        arabic_diacritics = re.compile("""
                                    ّ    | # Shadda
                                    َ    | # Fatha
                                    ً    | # Tanwin Fath
                                    ُ    | # Damma
                                    ٌ    | # Tanwin Damm
                                    ِ    | # Kasra
                                    ٍ    | # Tanwin Kasr
                                    ْ    | # Sukun
                                    ـ     # Tatwil/Kashida
                                """, re.VERBOSE)

        logging.info('Added Variables')
        logging.error('Error encountered: {error_message}')


        def click_button(browser, xpath):

            # delaying script to load webpage content
            time.sleep(2)

            # finding button using the xpath
            button = browser.find_element(xpath)

            # scrolling button into view and clicking it
            browser.execute_script("arguments[0].scrollIntoView();", button)
            browser.execute_script("arguments[0].click();", button)

        def check_activity_type(card, relevant_activity_list):
            headers = card.find("div",{"class":"tender-metadata border-left border-bottom"})
            main_activity_type = headers.find_all("span")[2].get_text().strip()
            if main_activity_type in relevant_activity_list: 
                return True
            else:
                return False

        def check_date(card,enddate): 
            headers = card.find("div",{"class":"tender-metadata border-left border-bottom"})
            # finding all header span tags
            headers_span = headers.find_all("span")
            date_published = headers_span[0].get_text().strip()
            date_published = datetime.strptime(date_published, '%Y-%m-%d').date()
            if enddate is None:
                enddate = datetime.now()
        
            return date_published >= enddate

        def parse_cards_html(card):

            headers = card.find("div",{"class":"tender-metadata border-left border-bottom"})
            
            # finding all header span tags
            headers_span = headers.find_all("span")
            date_published = headers_span[0].get_text().strip()
            rfp_type = headers_span[1].get_text().strip()
            main_activity_type = headers_span[2].get_text().strip()

            # finding all header h3 tags
            headers_h3 = headers.find("h3")
            rfp_name = headers_h3.get_text().strip()

            # finding header p tags
            headers_p = headers.find("p")
            entity_name = headers_p.get_text().strip()
            details_link = headers_p.find("a")["href"]

            # finding days left
            days_left = card.find("div",{"class":"text-center text-chart-indicator"}).get_text().strip()

            date_references = card.find("div",{"class":"tender-date border-left"})

            # finding all span tags in date_references
            date_references_span = date_references.find_all("span")
            reference_number = date_references_span[0].get_text().strip()
            last_date_clarifications = date_references_span[1].get_text().strip()
            last_date_submission = date_references_span[3].get_text().strip()
            
            if len(date_references_span) == 9:
                review_start = date_references_span[6].get_text().strip()
            else:
                review_start = ""
            
            # finding rfp price
            rfp_price = card.find("div",{"class":"tender-coast"}).find("span").get_text().strip()

            output_li = [date_published, rfp_type, main_activity_type, rfp_name, entity_name, details_link, days_left, reference_number, last_date_clarifications, last_date_submission, review_start, rfp_price]

            return output_li

        def scrape_description_and_state(url):
            """"
            Input: details_link as input
            Output: String of description of tender. If not found returns Not Found or retruns error if connection was unsuccessful. 
            """
            url = 'https://tenders.etimad.sa' +url
            time.sleep(5) 
            try:
                response = requests.get(url, timeout=25, verify=False)
                response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
                soup = BeautifulSoup(response.content, 'html.parser')
                purpose_span = soup.find('span', id='purposeSpan')
                description = purpose_span.text if purpose_span else "Not Found"

                state_element = soup.find('ul', class_='list-group form-details-list')
                if state_element: 
                    state_element=  state_element.find_all('li')[5].find('span')
                    state = state_element.text.strip() if state_element else "Not Found"
                else:
                    state = "Not Found"
                return description, state

                
            except requests.RequestException as e:
                return str(e),''

        def scrape_bidders(url):
            """"
            Input: details_link as input
            Opens the awardingStep tab using webdriver. 
            Output: Returns list of all ROWS <tr> found on the awardingStep page. If not table exists then the tender is not closed and no data is available. 
            """
            opts = FirefoxOptions()
            opts.add_argument("--headless")
            driver = webdriver.Firefox(options=opts)
                
            try:
                full_url = 'https://tenders.etimad.sa' + url
                driver.get(full_url)

                # Wait for the clickable element and click
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "awardingStepTab"))).click()
                # Wait for some time to ensure the table is loaded
                time.sleep(5)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                tr_elements = soup.find_all('tr')
                driver.quit()
                bidders_data = []
                for tr in tr_elements:
                    # Find all 'td' and 'th' elements within this 'tr'
                    td_th_elements = tr.find_all('td') + tr.find_all('th')

                    # Extract text from each 'td' or 'th' element
                    td_texts = [element.get_text(strip=True) for element in td_th_elements]

                    # Skip header rows
                    if all(element.name == 'th' for element in td_th_elements):
                        continue

                    bidders_data.append(td_texts)
                
                return bidders_data

            except Exception as e:
                print(f"An error occurred while scraping {url}: {e}")
                return None

        def clean_scraped_text(row):
                #If rfp description is empty then return the rfp name

            if pd.isnull(row['rfp_description']) or row['rfp_description'] == "Not Found":
                return row['rfp_name']
            else:
                return row['rfp_description']

        def diff_date(start_date,end_date):
            return (end_date-start_date).days

        def check_for_positive_keywords(card, positive_keywords): 
            headers = card.find("div",{"class":"tender-metadata border-left border-bottom"})
            headers_h3 = headers.find("h3")
            rfp_name = headers_h3.get_text().strip()

            if any(keyword.lower() in rfp_name  for keyword in positive_keywords): 
                return True
            else:
                return False

        def check_for_negative_keywords(card, negative_keywords): 
            headers = card.find("div",{"class":"tender-metadata border-left border-bottom"})
            headers_h3 = headers.find("h3")
            rfp_name = headers_h3.get_text().strip()

            if any(keyword.lower() in rfp_name  for keyword in negative_keywords): 
                return True
            else:
                return False

        def preprocess_text(string_text):
            if string_text is None:
                string_text = " "
            lst = []
            punctuations = '''`÷×؛<>_-()*&^%][ـ،/:"؟.,،'{}~¦+|!”…“–ـ''' + string.punctuation
            stop_words_arabic = stp.stopwords_list()
            extra_stopwords = ['الى', 'مشروع', 'المشروع', 'الهيئة', 'المركز', 'العمل', 'الوطني', 'المملكة', 'برنامج', 'السعودية']
            stop_words_arabic.extend(extra_stopwords)

            for text in string_text.split():
                # Remove punctuations
                translator = str.maketrans(punctuations, ' ' * len(punctuations))
                text = text.translate(translator)

                # Remove Tashkeel
                text = re.sub(arabic_diacritics, '', text)
                text = re.sub('[A-Za-z0-9]',' ',text)
                
                # Stemming
                st = ISRIStemmer()
                text = st.stem(text)

                # Normalize Arabic characters
                text = re.sub("[إأآا]", "ا", text)
                text = re.sub("ى", "ي", text)
                text = re.sub("ؤ", "ء", text)
                text = re.sub("ئ", "ء", text)
                text = re.sub("ة", "ه", text)
                text = re.sub("گ", "ك", text)

                # Remove stop words
                if text not in stop_words_arabic:
                    lst.append(text)

            return ' '.join(lst)

        def scrape_today(scraped_data, enddate): 
            # launching webdriver and routing to website url    
            opts = FirefoxOptions()
            opts.add_argument("--headless")
            browser = webdriver.Firefox(options=opts)

            url = "https://tenders.etimad.sa/Tender/AllTendersForVisitor"
            browser.get(url)

            # increase number of records per page
            time.sleep(2)
            dropdown_element = browser.find_element("xpath", '//*[@id="itemsPerPage"]')
            dropdown = Select(dropdown_element)
            dropdown.select_by_value('24')


            # instantiating flow control variables
            page_counter = 0
            keep_switching = True

            # switching pages until last page is reached
            while keep_switching:
                time.sleep(1.5)
                # get html content of page
                html = browser.page_source
                soup = BeautifulSoup(html,features="lxml")
                cards = soup.find_all("div",{"class":"tender-card rounded card mt-0 mb-0"})

                for card in cards:
                    if check_activity_type(card,relevant_activity_list):
                        output_li = parse_cards_html(card=card) 
                        for i,k in enumerate(scraped_data.keys()):
                            scraped_data[k].append(output_li[i])
                
                #Pagination
                if page_counter == 0:
                    element = browser.find_element("xpath",'//*[@id="cardsresult"]/div[2]/div/nav/ul/li[5]/button')
                else:
                    element = browser.find_element("xpath",'//*[@id="cardsresult"]/div[2]/div/nav/ul/li[6]/button')
                if element.is_enabled():
                    browser.execute_script("arguments[0].scrollIntoView();", element)
                    browser.execute_script("arguments[0].click();", element)
                else:
                    keep_switching = False

                page_counter += 1

                if not check_date(card,enddate):
                    keep_switching=False

            browser.quit()

            json_data = json.dumps(scraped_data,ensure_ascii=False, indent=2)
            return json_data

        def clean_bidder_info(df):
            # Load the Excel file
            if df.empty:
                # Return the original DataFrame if it's empty
                return df
            
            
            # Normalize 'Matching (Y/N)'
            df['Matching (Y/N)'] = df['Matching (Y/N)'].apply(lambda x: 'Y' if x == 'مطابق' else ('N' if x == 'غير مطابق' else x))
            
            # Remove unnecessary columns
            columns_to_drop = ['Winning Company Name', 'Winning Company Bid Value',]
            df_cleaned = df.drop(columns=columns_to_drop)
            return df_cleaned 

        def get_categorization(df): 

            df['rfp_description'] = df.apply(lambda row: re.sub(r'HTTPS.*', row['rfp_name'], row['rfp_description'], flags=re.IGNORECASE), axis=1)

            # Step 2: Remove specific patterns ("_x000D_", "...", "\n")
            df['rfp_description'] = df['rfp_description'].apply(lambda x: re.sub(r'_x000D_|\.\.\.|\n', '', x))

            # Step 3: Replace more than 3 spaces with 1 space
            df['rfp_description'] = df['rfp_description'].apply(lambda x: re.sub(r' {3,}', ' ', x))

            # Step 4: Replace "عرض الأقل" with nothing
            df['rfp_description'] = df['rfp_description'].apply(lambda x: x.replace("عرض الأقل", ""))

            # Combine text columns for TF-IDF vectorization and apply preprocessings
            df['combined_text'] = df['rfp_name'].apply(preprocess_text) + " " + df['rfp_description'].apply(preprocess_text)

            X_test = tfidf.transform(df['combined_text']).toarray()
            new_predictions = pickled_model.predict(X_test)
            predicted_labels = label_encoder.inverse_transform(new_predictions)

            return predicted_labels

        def write_df_to_blob(df, blob_name):
            with io.BytesIO() as output_stream:
                with pd.ExcelWriter(output_stream, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False)
                output_stream.seek(0)  # Go to the beginning of the stream
                container_client.upload_blob(blob_name, output_stream, overwrite=True)

        def read_excel_from_blob(blob_name):
            blob_client = container_client.get_blob_client(blob_name)
            with io.BytesIO() as blob_io:
                blob_data = blob_client.download_blob()
                blob_data.readinto(blob_io)
                blob_io.seek(0)  # Go to the beginning of the stream
                df = pd.read_excel(blob_io)
            return df

        def load_pickle_from_blob(blob_name):
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob()
            return pickle.loads(blob_data.readall())

        logging.info('Added Functions')
        logging.error('Error encountered: {error_message}')

        start_date = datetime.now().date()
        end_date = start_date - timedelta(days=3)

        logging.info('Added Functions')
        logging.info(f'Start date: {start_date}')
        logging.info(f'End date: {end_date}')


        logging.info('Scrape Today')

        json_data = scrape_today(scraped_data, end_date)
        df = pd.read_json(json_data)
        logging.error('Error encountered: {error_message}')
        logging.info('Scrape Complete.')


        logging.info('Scraping Tender Descriptions.')

        df['rfp_description'], df['rfp_state'] = zip(*df['details_link'].apply(scrape_description_and_state))
        df['rfp_description'] = df.apply(clean_scraped_text, axis=1)
        logging.error('Error encountered: {error_message}')
        logging.info('Scrape Complete.')


        logging.info('Scraping Bidder Information')
        all_bidders_info = []
        # Iterate through each row in df
        for _, row in df.iterrows():
                # Check if tender is closed
                if row['rfp_state'] == "تم اعتماد الترسية":
                    bidders = scrape_bidders(row['details_link'])
                    if bidders: 
                        # Separate winning bid (last entry) and other bids
                        winning_bid = bidders[-1]
                        other_bids = bidders[:-1]

                        # Add winning bid information
                        all_bidders_info.append({
                            'Reference Number': row['reference_number'],
                            'Winning Company Name': winning_bid[0],
                            'Winning Company Bid Value': winning_bid[1],
                            'Awarded Value': winning_bid[2],
                            'Company Name': winning_bid[0],
                            'Company Bid Value': winning_bid[1],
                            'Matching (Y/N)': None,
                            'Bid Type Info': 'Winner'
                        })

                        # Add other bids information
                        for bid in other_bids:
                            all_bidders_info.append({
                                'Reference Number': row['reference_number'],
                                'Winning Company Name': None,
                                'Winning Company Bid Value': None,
                                'Awarded Value': None,
                                'Company Name': bid[0],
                                'Company Bid Value': bid[1],
                                'Matching (Y/N)': bid[2],
                                'Bid Type Info': 'Bidding'
                            })

        logging.error('Error encountered: {error_message}')
        df_bidders = pd.DataFrame(all_bidders_info)
        logging.info('Scrape Complete.')


        logging.info('Categorizing Tenders.')







        # Use the function to read your Excel files
        Full_Final_Tableau_blob_name = 'withTFIDF.xlsx'
        Full_Bidders_Tablea_blob_name = 'all_bidders_info_final_cleaned_final.xlsx'

        df_full_final = read_excel_from_blob(Full_Final_Tableau_blob_name)
        df_bidders_final = read_excel_from_blob(Full_Bidders_Tablea_blob_name)



        pickled_model_blob_name = 'xgb_model_best_estimator.pkl'
        tfidf_blob_name = 'tfidf_vectorizer.pkl'
        label_encoder_blob_name = 'label_encoder.pkl'
        pickled_model = load_pickle_from_blob(pickled_model_blob_name)
        tfidf = load_pickle_from_blob(tfidf_blob_name)
        label_encoder = load_pickle_from_blob(label_encoder_blob_name)

        df['Manual Category'] = get_categorization(df)
        df.drop('combined_text', axis=1, inplace=True)
        logging.error('Error encountered: {error_message}')
        logging.info('Categorizing Complete.')


        logging.info('Exporting Data.')
        logging.error('Error encountered: {error_message}')

        df_bidders = clean_bidder_info(df_bidders)
        df_full_final = pd.concat([df_full_final, df], ignore_index=True)
        df_bidders_final = pd.concat([df_bidders_final, df_bidders], ignore_index=True)

        df_full_final.drop_duplicates(inplace=True)
        df_bidders_final.drop_duplicates(inplace=True)

        df_full_final.to_excel('withTFIDF.xlsx', index=False)
        df_bidders_final.to_excel('all_bidders_info_final_cleaned_final.xlsx', index= False)

        logging.info(f"Completed!")
        logging.error('Error encountered: {error_message}')

        df['TenderID'] = df['details_link'].apply(lambda x: x.split('?STenderId=')[1] if '?STenderId=' in x else None)
                            
        df['Tender URL'] = df['details_link'].apply(lambda x: 'https://tenders.etimad.sa' + x if x else None)

        df['Full Detailed URL'] = df['TenderID'].apply( lambda x: 'https://tenders.etimad.sa/Tender/PrintConditionsTemplateHtmlWithVersion?' + x if x else None)

        columns_to_keep = ['reference_number', 
                        'rfp_name',
                        'rfp_description',
                        'entity_name',
                        'Manual Category',
                        'date_published',
                        'last_date_submission',
                        'rfp_price',
                        'Tender URL',
                        'Full Detailed URL' ]

        share_df = df[columns_to_keep]
        share_df = df[df['Manual Category'] != 'Other']

        if share_df.empty: 
            logging.info("No relevant tenders")
            logging.error('Error encountered: {error_message}')
        else:
            write_df_to_blob(share_df, f'Related Tenders - {end_date}.xlsx')

            logging.info("Tenders are exported")
            logging.error('Error encountered: {error_message}')

        write_df_to_blob(df_full_final, 'df_full_final.xlsx')
        write_df_to_blob(df_bidders_final, 'df_bidders_final.xlsx')
        logging.info('Python timer trigger function executed.')