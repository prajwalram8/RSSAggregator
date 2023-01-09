import win32com.client as win32

outlook = win32.Dispatch('outlook.application')
mail = outlook.CreateItem(0)
mail.Subject = 'Test News Letter'
mail.To = "vaseem.sg@gagroup.net"

try:
    with open('templates/sample_out_Automotive.html', 'r') as f:
        html_string = f.read()
    print("HTML template found. Using it for the body of the mail")
except FileNotFoundError:
    print("HTML template file not found. Setting html string to default template")
    html_string = "Please contact the admin"

mail.HTMLBody = html_string
mail.Send()