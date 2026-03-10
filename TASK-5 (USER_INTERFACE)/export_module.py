from fpdf import FPDF
import matplotlib.pyplot as plt
import os

def export_pdf(
    df,
    total_csv_records,
    total_chunks,
    avg_score,
    unique_uids,
    positive,
    negative,
    neutral
):

    os.makedirs("report", exist_ok=True)

    # Pie Chart
    pie_path = "report/pie_chart.png"

    plt.figure()
    plt.pie(
        [positive, negative, neutral],
        labels=["Positive","Negative","Neutral"],
        autopct="%1.0f%%"
    )
    plt.title("Sentiment Distribution")
    plt.savefig(pie_path)
    plt.close()

    # Histogram 
    hist_path = "report/hist_chart.png"

    if "score" in df.columns:

        plt.figure()
        plt.hist(df["score"], bins=10)
        plt.title("Score Histogram")
        plt.xlabel("Score")
        plt.ylabel("Frequency")
        plt.savefig(hist_path)
        plt.close()

    else:
        hist_path = None

    # Create PDF 
    pdf = FPDF()
    pdf.add_page()

    pdf.set_font("Arial", "B", 16)
    pdf.cell(0,10,"Parallel Text Processing Report", ln=True)

    pdf.set_font("Arial","",12)
    pdf.ln(5)

    pdf.cell(0,10,f"Total CSV Records: {total_csv_records}", ln=True)
    pdf.cell(0,10,f"Chunks Stored: {total_chunks}", ln=True)
    pdf.cell(0,10,f"Average Score: {round(avg_score,2)}", ln=True)
    pdf.cell(0,10,f"Unique UIDs: {unique_uids}", ln=True)

    pdf.ln(5)

    pdf.cell(0,10,f"Positive: {positive}", ln=True)
    pdf.cell(0,10,f"Negative: {negative}", ln=True)
    pdf.cell(0,10,f"Neutral: {neutral}", ln=True)

    pdf.ln(10)

    pdf.image(pie_path, w=150)

    if hist_path:
        pdf.ln(10)
        pdf.image(hist_path, w=150)

    pdf_path = "report/view_records_report.pdf"
    pdf.output(pdf_path)

    return pdf_path