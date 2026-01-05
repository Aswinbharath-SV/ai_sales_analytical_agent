import pandas as pd
import PyPDF2

def process_file(file_path):
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.endswith((".xlsx",".xls")):
        df = pd.read_excel(file_path)
    elif file_path.endswith(".pdf"):
        text = extract_text_from_pdf(file_path)
        df = parse_pdf_to_dataframe(text)
    else:
        raise ValueError("Unsupported file type")
    expected_columns = ['date','region','product','revenue']
    for col in expected_columns:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = pd.to_numeric(df['revenue'])
    return df

def extract_text_from_pdf(pdf_path):
    text=""
    with open(pdf_path,'rb') as f:
        reader=PyPDF2.PdfReader(f)
        for page in reader.pages:
            text+=page.extract_text()+"\n"
    return text

def parse_pdf_to_dataframe(text):
    lines=[line.split(',') for line in text.split('\n') if line.strip()]
    df=pd.DataFrame(lines[1:], columns=lines[0])
    df['revenue']=pd.to_numeric(df['revenue'])
    df['date']=pd.to_datetime(df['date'])
    return df
