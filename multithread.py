import mysql.connector
import requests
import json
from bs4 import BeautifulSoup
import concurrent.futures
# Website to scrape
URL = "https://www.thenational.academy/pupils/years"

conn = mysql.connector.connect(
        host="localhost",
        user="root ",
        password="",
        database="test_academy"
    )
cursor = conn.cursor()


def create_db():
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS lessons (id INT AUTO_INCREMENT PRIMARY KEY,unit_id TEXT,lesson_name TEXT,url TEXT)""")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS lesson_data (id INT AUTO_INCREMENT PRIMARY KEY,year TEXT,subject TEXT,unit_slug TEXT,url TEXT,content JSON)""")
    return

def get_year_urls(url):
    year_urls = ["/pupils/years/year-10/subjects", "/pupils/years/year-11/subjects"]
    return year_urls

def get_sub_urls(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]
        return links

    except requests.RequestException as e:
        print(f"Error fetching {url}")
        return []

def get_exam_sub_urls(url):
    try:
        exam_type = ["/examboard/aqa","/examboard/ocr","/examboard/edexcel"]
        links = []        
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        get_page_links = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]

        if len(get_page_links) > 0:
            for exam_unit_url in get_page_links:
                    if exam_unit_url.endswith("/units"):
                        try:
                            exam_unit_full_url = "https://www.thenational.academy" + exam_unit_url
                            response = requests.get(exam_unit_full_url)
                            response.raise_for_status()
                            soup = BeautifulSoup(response.text, "html.parser")
                            sub_exam_lesson_links = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]
                            urls_with_lessons = [url for url in sub_exam_lesson_links if url.endswith("/lessons")]
                            links.extend(urls_with_lessons)
                        except requests.RequestException as e:
                            print(f"Error fetching {url}")
                            continue
                    if exam_unit_url.endswith("/lessons"):
                        links.append(exam_unit_url)
        else:
            for exam in exam_type:
                try:
                    sub_url = url + exam
                    response = requests.get(sub_url)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")
                    sub_exam_unit_links = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]
                    for exam_unit_url in sub_exam_unit_links:
                        try:
                            exam_unit_full_url = "https://www.thenational.academy" + exam_unit_url
                            response = requests.get(exam_unit_full_url)
                            response.raise_for_status()
                            soup = BeautifulSoup(response.text, "html.parser")
                            sub_exam_lesson_links = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]
                            links.extend(sub_exam_lesson_links)
                        except requests.RequestException as e:
                            print(f"Error fetching {url}")
                            continue
                except requests.RequestException as e:
                    print(f"Error fetching {url}")
                    continue
                    

        return links

    except requests.RequestException as e:
        print(f"Error fetching {url}")
        return []

def get_lesson_urls(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        links = [b["href"] for a in soup.find_all("div", role="listitem") for b in a.find_all("a", href=True) if '/pupils/programmes/' in b["href"]]
        return links

    except requests.RequestException as e:
        print(f"Error fetching {url}")
        return []


    

def save_year_urls(urls):
    try:
        for index, url in enumerate(urls, start=10):
            full_url = "https://www.thenational.academy" + url
            try:
                sub_urls = get_sub_urls(full_url)
                save_subject_urls(sub_urls)
                index += 1
            except mysql.connector.Error as err:
                print(f"Error inserting {url}: {err}")

        print("✅ URLs saved successfully!")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    

def save_subject_urls(subject_urls):
    try:
        base_url = "https://www.thenational.academy"
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_url = {
                executor.submit(get_exam_sub_urls, base_url + url): url
                for url in subject_urls
            }
            for future in concurrent.futures.as_completed(future_to_url):
                original_url = future_to_url[future]
                subject_url = base_url + original_url
                try:
                    unit_urls = future.result()
                    if unit_urls:
                        print(len(unit_urls), subject_url)
                        save_unit_urls(unit_urls)
                except mysql.connector.Error as err:
                    print(f"Database error inserting {original_url}: {err}")
                    continue
                except Exception as err:
                    print(f"Error processing {original_url}: {err}")
                    continue
    except mysql.connector.Error as err:
        print(f"Database error outside future tasks: {err}")
    return

def save_unit_urls(unit_urls):
    try:
        for index, url in enumerate(unit_urls, start=0):
            unit_url = "https://www.thenational.academy" + url
            try:
                lesson_urls = get_lesson_urls(unit_url)
                print(len(lesson_urls),"____", unit_url)
                save_lesson_urls(lesson_urls)

            except mysql.connector.Error as err:
                print(f"Error inserting {url}: {err}")
                continue
    except mysql.connector.Error as err:
        print(f"Error inserting {url}: {err}")
    return


def save_lesson_page_content(urls):
    try:
        for url in urls:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

            if script_tag:
                json_content = script_tag.text;
            else:
                json_content = {}

            cursor.execute("INSERT INTO lesson_data (url,content) VALUES (%s,%s)", (url, json_content))
            conn.commit()
    except requests.RequestException as e:
            print(f"Error fetching {url}")
                    

def save_lesson_urls(lesson_urls):
    try:
        for index, url in enumerate(lesson_urls, start=0):
            lesson_url = "https://www.thenational.academy" + url
            try:
                cursor.execute("INSERT INTO lessons (unit_id,lesson_name,url) VALUES (%s,%s,%s)", ("", "",lesson_url,))
                conn.commit()
            except mysql.connector.Error as err:
                print(f"Error inserting {url}: {err}")
                continue
    except mysql.connector.Error as err:
        print(f"Error inserting {url}: {err}")
    return




def process_lesson_url(url):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="test_academy"
        )
        cursor = conn.cursor()
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        json_content = script_tag.text if script_tag else ""
        cursor.execute(
            "INSERT INTO lesson_data (url, content) VALUES (%s, %s)",
            (url, json_content)
        )
        conn.commit()
        print("------------>",url)
    except mysql.connector.Error as db_err:
        print(f"[DB Error] {url}: {db_err}")
    except requests.RequestException as net_err:
        print(f"[Network Error] {url}: {net_err}")
    except Exception as e:
        print(f"[General Error] {url}: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def save_lesson_page_content_concurrent(lesson_urls, max_workers=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_lesson_url, url) for url in lesson_urls]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result() 
            except Exception as e:
                print(f"Unhandled error in thread: {e}")
    print("✅ Concurrent fetching and saving of lesson data complete!")



def main():
    create_db()
    year_urls = get_year_urls(URL)
    if not year_urls:
        print("No URLs found.")
        return
    save_year_urls(year_urls)
    cursor.execute("SELECT url FROM lessons")
    lesson_urls = [row[0] for row in cursor.fetchall()]
    save_lesson_page_content_concurrent(lesson_urls, max_workers=20)
    print("✅ JSON DATA saved successfully!")
    cursor.close()
    conn.close()

main()


