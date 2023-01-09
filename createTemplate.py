import pathlib
import pandas as pd
from datetime import date
from jinja2 import Environment, FileSystemLoader


df = pd.read_csv("TempStage/GOOGLEALERTS_TEMP_STAGE/GOOGLEALERTS_October_24_2022.csv", sep='\t')
df = df[['title', 'link', 'pub_date', 'summary', 'alertFor','direct_link']].copy()
df.reset_index(inplace=True)

def generate_template(df):
    today_date = date.today().strftime("%B %d, %Y")
    
    for each in df['alertFor'].unique():

        ref_df = df[df['alertFor'] == each].copy()

        items  = ref_df.to_dict('records')

        environment = Environment(loader=FileSystemLoader("templates/"))

        result_filename = f"templates/sample_out_{each}.html"

        template = environment.get_template("html3.html")

        content = template.render(
            items=items,
            theme=each,
            today_date=today_date

        )
        
        with open(result_filename, mode="w", encoding="utf-8") as message:
            message.write(content)
            print(f"... wrote {result_filename}")
        
    return None

if __name__ == "__main__":
    generate_template(df)

