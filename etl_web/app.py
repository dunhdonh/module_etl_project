from flask import Flask, render_template
import time
import os
import shutil

app = Flask(__name__)

# Nếu ETL output sang một nơi khác, copy vào static
OUTPUT_SRC = "/mnt/d/module_etl/output_data/temperature_plot.png"
STATIC_DST = os.path.join(app.root_path, "static", "temperature_plot.png")

def update_plot():
    if os.path.exists(OUTPUT_SRC):
        shutil.copy(OUTPUT_SRC, STATIC_DST)

@app.route("/")
def index():
    update_plot()
    return render_template("index.html", timestamp=int(time.time()))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
