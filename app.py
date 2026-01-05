from flask import Flask, render_template, request
from analytics import process_file
from llm import extract_query_details, explain_result
import os

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.route("/", methods=["GET", "POST"])
def index():
    answer = ""
    chart_data = {}
    if request.method == "POST":
        file = request.files.get("file")
        question = request.form.get("question")
        if file:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(file_path)
            df = process_file(file_path)
            parsed = extract_query_details(question)
            region = parsed.get("region")
            df_region = df[df['region']==region].copy()
            df_region['quarter'] = df_region['date'].dt.to_period("Q")
            revenue_by_quarter = df_region.groupby('quarter')['revenue'].sum()
            chart_data = {
                'quarters': [str(q) for q in revenue_by_quarter.index],
                'revenues': [int(r) for r in revenue_by_quarter.values]
            }
            answer = explain_result(revenue_by_quarter.to_dict())
    return render_template("index.html", answer=answer, chart_data=chart_data)

if __name__ == "__main__":
    app.run(debug=True)
