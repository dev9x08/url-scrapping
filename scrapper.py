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
        database="academy"
    )
cursor = conn.cursor()

def create_db():
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS years (id INT AUTO_INCREMENT PRIMARY KEY,name TEXT,url TEXT UNIQUE )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS subjects (id INT AUTO_INCREMENT PRIMARY KEY,year_id TEXT,subject_name TEXT,url TEXT)""")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS units (id INT AUTO_INCREMENT PRIMARY KEY,subject_id TEXT,unit_name TEXT,url TEXT)""")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS exam_boards (id INT AUTO_INCREMENT PRIMARY KEY,unit_id TEXT, exam_name TEXT,url TEXT)""")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS levels (id INT AUTO_INCREMENT PRIMARY KEY,unit_id TEXT, exam_id TEXT,level_name TEXT,url TEXT)""")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS lessons (id INT AUTO_INCREMENT PRIMARY KEY,unit_id TEXT,lesson_name TEXT,url TEXT)""")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS lesson_data (id INT AUTO_INCREMENT PRIMARY KEY,url TEXT,content LONGTEXT)""")
    return

def get_year_urls(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        year_urls = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/years/' in a["href"]]
        return year_urls

    except requests.RequestException as e:
        print(f"Error fetching  {url}: {e} in Year Url")
        return []

def get_sub_urls(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]
        return links

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []

def get_exam_sub_urls(url):
    try:
        exam_type = ["/examboard/aqa","/examboard/ocr","/examboard/edexcel"]
        links = []
        for exam in exam_type:
            sub_url = url + exam
            response = requests.get(sub_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            sub_exam_unit_links = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]
            for exam_unit_url in sub_exam_unit_links:
                exam_unit_full_url = "https://www.thenational.academy" + exam_unit_url
                response = requests.get(exam_unit_full_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                sub_exam_lesson_links = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]
                links.extend(sub_exam_lesson_links)
        return links

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []

def get_lesson_urls(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        links = [a["href"] for a in soup.find_all("div", role="listitem") for a in a.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]
        return links

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []



def get_subject_names(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        subject_names = [item.find_all("div")[1].get_text(strip=True) for item in soup.find_all("div", role="listitem")]
        return subject_names

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []
    
def get_unit_names(url, year_id):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        unit_names = [item.find_all("div")[3].get_text(strip=True) for item in
                      soup.find_all("a", href=lambda href: href and '/pupils/programmes/' in href) if
                      len(item.find_all("div")) > 2]
        if len(unit_names) == 0:
            if url.endswith("/options") == False:
                response = requests.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                unit_names = [
                    item.find("div").find("span").get_text(strip=True)
                    for item in soup.find_all("a", href=lambda href: href and '/pupils/programmes/' in href)
                    if item.find("div") and item.find("div").find("span")
                ]
        return unit_names

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []


def get_exam_unit_names(url, year_id):
    try:
        exam_type = ["/examboard/aqa","/examboard/ocr","/examboard/edexcel"]
        unit_names = []
        for exam in exam_type:
            sub_url = url + exam
            response = requests.get(sub_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            sub_exam_unit_links = [a["href"] for a in soup.find_all("a", href=True) if '/pupils/programmes/' in a["href"]]

            for exam_unit_link in sub_exam_unit_links:
                exam_unit_full_link = "https://www.thenational.academy" + exam_unit_link
                response = requests.get(exam_unit_full_link)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                sub_exam_unit_names = [item.find_all("div")[3].get_text(strip=True) for item in
                      soup.find_all("a", href=lambda href: href and '/pupils/programmes/' in href) if
                      len(item.find_all("div")) > 2]
                unit_names.extend(sub_exam_unit_names)
        return unit_names

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []
    

def get_lesson_names(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        lesson_names = [item.find_all("div")[3].get_text(strip=True) for item in
                      soup.find_all("a", href=lambda href: href and '/pupils/programmes/' in href) if
                      len(item.find_all("div")) > 2]
        return lesson_names
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []
    
    

def save_year_urls(urls):
    try:
        for index, url in enumerate(urls, start=1):
            full_url = "https://www.thenational.academy" + url
            name = f"year{index}"
            try:
                cursor.execute("INSERT IGNORE INTO years(name,url) VALUES (%s, %s)", (name, full_url,))
                conn.commit()

                sub_urls = get_sub_urls(full_url)
                subject_names = get_subject_names(full_url)
                save_subject_urls(sub_urls, subject_names, index)
                index += 1
            except mysql.connector.Error as err:
                print(f"Error inserting {url}: {err}")

        print("✅ URLs saved successfully!")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    

def save_subject_urls(subject_urls, subject_names, year_id):
    try:
        for index, url in enumerate(subject_urls, start=0):
            subject_url = "https://www.thenational.academy" + url
            subject_name = subject_names[index]

            try:
                cursor.execute("INSERT INTO subjects (year_id,subject_name,url) VALUES (%s,%s,%s)", (year_id, subject_name, subject_url,))
                conn.commit()

                subject_id = cursor.lastrowid
                if year_id == 10 or year_id == 11:
                    if subject_url.endswith("/options"):
                        unit_urls = get_exam_sub_urls(subject_url)
                        unit_names = get_exam_unit_names(subject_url,year_id)
                    else:
                        unit_urls = get_sub_urls(subject_url)
                        unit_names = get_unit_names(subject_url,year_id)
                    save_unit_urls(subject_id,unit_urls, unit_names, year_id)
                else:
                    unit_urls = get_sub_urls(subject_url)
                    unit_names = get_unit_names(subject_url,year_id)
                    save_unit_urls(subject_id,unit_urls, unit_names, year_id)
            except mysql.connector.Error as err:
                print(f"Error inserting {url}: {err}")

    except mysql.connector.Error as err:
        print(f"Error inserting {url}: {err}")
    return

def save_unit_urls(subject_id, unit_urls, unit_names, year_id):
    try:
        for index, url in enumerate(unit_urls, start=0):
            unit_url = "https://www.thenational.academy" + url
            unit_name = unit_names[index]
            try:
                cursor.execute("INSERT INTO units (subject_id, unit_name, url) VALUES (%s,%s,%s)",
                               (subject_id, unit_name, unit_url,))
                conn.commit()
                unit_id = cursor.lastrowid
                lesson_urls = get_lesson_urls(unit_url)
                lesson_names = get_lesson_names(unit_url)
                save_lesson_urls(unit_id, lesson_urls, lesson_names)

            except mysql.connector.Error as err:
                print(f"Error inserting {url}: {err}")
    except mysql.connector.Error as err:
        print(f"Error inserting {url}: {err}")
    return


def save_lesson_page_content(urls):
    for url in urls:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

        if script_tag:
            json_content = script_tag.text;
        else:
            json_content = ""

        cursor.execute("INSERT INTO lesson_data (url,content) VALUES (%s,%s)", (url, json_content))
        conn.commit()

def save_lesson_urls(unit_id, lesson_urls, lesson_names):
    try:
        for index, url in enumerate(lesson_urls, start=0):
            lesson_url = "https://www.thenational.academy" + url
            lesson_name = lesson_names[index]
            try:
                cursor.execute("INSERT INTO lessons (unit_id,lesson_name,url) VALUES (%s,%s,%s)", (unit_id, lesson_name, lesson_url,))
                cursor.execute("INSERT INTO exam_boards (unit_id,exam_name,url) VALUES (%s,%s,%s)", (unit_id, "", "",))
                cursor.execute("INSERT INTO levels (unit_id,exam_id,level_name, url) VALUES (%s,%s,%s, %s)", (unit_id, "", "", ""))
                conn.commit()
            except mysql.connector.Error as err:
                print(f"Error inserting {url}: {err}")
    except mysql.connector.Error as err:
        print(f"Error inserting {url}: {err}")
    return


def main():
    create_db()
    year_urls = get_year_urls(URL)
    if year_urls:
        save_year_urls(year_urls)
        cursor.execute("SELECT url FROM lessons WHERE id > 17204")
        lesson_urls = [row[0] for row in cursor.fetchall()]
        save_lesson_page_content(lesson_urls)
        print("✅ JSON DATA saved successfully!")
    else:
        print("No URLs found.")

    cursor.close()
    conn.close()
main()

