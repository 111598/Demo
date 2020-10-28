import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

correspondenceId = "correspondence_id_1","correspondence_id_2",...,"correspondence_id_N"
swagger_url = "https://emea.datasite.com/swagger-ui.html?urls.primaryName=Q%26A"
projectId = "project_Id"

driver = webdriver.Chrome(
    executable_path="c:\\dev\\reprocess\\lib\\chromedriver.exe")
driver.set_window_size(1024, 600)
driver.maximize_window()
website_URL = "https://datasiteone.merrillcorp.com/global/projects"
driver.get(website_URL)
time.sleep(8)
driver.find_element_by_id("username").send_keys("userName")
driver.find_element_by_id("password").send_keys("password")
driver.find_element_by_class_name("primary").click()
time.sleep(12)
token = str(driver.execute_script('return window.localStorage.authToken'))
time.sleep(2)
driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 't')
website_URL = swagger_url
driver.get(website_URL)
time.sleep(6)
print(token)
driver.find_element_by_xpath('//*[@id="operations-tag-Correspondence_V6"]/a').click()
driver.execute_script("window.scrollTo(0,2000)")
driver.find_element_by_id("operations-Correspondence V6-deleteCorrespondenceUsingDELETE").click()
time.sleep(2)
driver.find_element_by_xpath(
    '//*[@id="operations-Correspondence V6-deleteCorrespondenceUsingDELETE"]/div[2]/div/div[1]/div[1]/div[2]/button').click()
time.sleep(2)
driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(1) input").click()
driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(1) input").clear()
driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(1) input").send_keys(str('Bearer ' + token))
time.sleep(2)
driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(2) input").clear()
print('Cleared projectId')
driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(3) input").clear()
driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(3) input").send_keys(projectId)
time.sleep(2)
for i in correspondenceId:
    driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(2) input").click()
    driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(2) input").clear()
    driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(2) input").send_keys(i)
    print(i)
    time.sleep(1)
    driver.find_element(By.CSS_SELECTOR, ".execute").click()
