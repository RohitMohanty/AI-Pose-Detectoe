from google_images_download import google_images_download

image= google_images_download.googleimagesdownload()
query = "iyengar yoga"

def downloader(searchterm):
    arguments= {"keywords":searchterm,
                "format": "jpg",
                "limit":1,
                "print_urls":True,
                "size":"large",
                "aspect_ratio":"square"}
    
    try:
        image.download(arguments)

    except FileNotFoundError:
         arguments= {"keywords":searchterm,
                "format": "jpg",
                "limit":10,
                "print_urls":True,
                "size":"large",
                }
    
    try:
        image.download(arguments)
    except:
        pass


downloader(query)
print()
