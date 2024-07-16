import google.generativeai as genai
GOOGLE_API_KEY = 'AIzaSyAud0VoF3zX2z93B1XA6LPcbwA3DG_IWcI'
genai.configure(api_key=GOOGLE_API_KEY)

model = genai.GenerativeModel('gemini-pro')

prompt = """Act as data analyst and suggest exactly 10 topics that can be searched using youtube api that will give distinct content creator for each query.
the result should be comma seperated, without any bullets or numbers and within 5 words"""
response = model.generate_content(prompt)
print(response.text)