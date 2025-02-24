import re,chardet
from icon_image_lib.LR import LR


# def get_original_images(html_bytes):
#     try:
#         detected_encoding = chardet.detect(html_bytes)['encoding']
#         soup = html_bytes.decode(detected_encoding)
#     except Exception as e:
#         print(e)
#         soup = html_bytes.decode('utf-8')
#
#     #print(f"type: { type(soup)}")
#
#     start_tag = 'FINANCE",[22,1]]]]]'
#     end_tag = ':[null,null,null,1,['
#     # with open('text.html', 'w', encoding='utf-8') as file:
#     #      file.write(soup)
#     matched_google_image_data = LR().get(soup, start_tag, end_tag)
#     if 'Error' in matched_google_image_data:
#         return None
#     if not matched_google_image_data:
#         print('No matched_google_image_data')
#         return (['No start_tag or end_tag'],['No start_tag or end_tag'],['No start_tag or end_tag'])
#     matched_google_image_data = str(matched_google_image_data).replace('\u003d','=')
#     matched_google_image_data = str(matched_google_image_data).replace('\u0026', '&')
#
#     print(matched_google_image_data)
#     print(type(matched_google_image_data))
#
#     # thumbnails = [
#     #     bytes(bytes(thumbnail, 'utf-8').decode("unicode-escape"), "utf-8").decode("unicode-escape") for thumbnail in
#     #     matched_google_image_data
#     # ]
#     # print(thumbnails)
#     thumbnails = matched_google_image_data
#     if '"2003":' not in thumbnails:
#         print('No 2003 tag found')
#         return (['No google image results found'],['No google image results found'],['No google image results found'])
#     # matched_google_images_thumbnails = ", ".join(
#     #     re.findall(r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]',
#     #                str(thumbnails))).split(", ")
#
#     regex_pattern_desc = r'"2003":\[null,"[^"]*","[^"]*","(.*?)"'
#     # print(matched_google_images_thumbnails)
#     matched_description = re.findall(regex_pattern_desc, str(thumbnails))
#
#     regex_pattern_src = r'"2003":\[null,"[^"]*","(.*?)"'
#     matched_source = re.findall(regex_pattern_src, str(thumbnails))
#     #print(matched_source)
#     removed_matched_google_images_thumbnails = re.sub(
#         r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', "", str(thumbnails))
#
#     # Extract full resolution images
#     matched_google_full_resolution_images = re.findall(r"(?:|,),\[\"(https:|http.*?)\",\d+,\d+\]",
#                                                        removed_matched_google_images_thumbnails)
#
#     #print(len(matched_description))
#
#     full_res_images = [
#         bytes(bytes(img, "utf-8").decode("unicode-escape"), "utf-8").decode("unicode-escape") for img in
#         matched_google_full_resolution_images
#     ]
#     cleaned_urls = [clean_image_url(url) for url in full_res_images]
#     cleaned_source = [clean_source_url(url) for url in matched_source]
#
#     # print(len(cleaned_descriptions))
#     # print(matched_description)
#     # Assume descriptions are extracted
#     # descriptions = LR().get(soup, '"2008":[null,"', '"]}],null,') # Replace 'description_pattern' with your actual regex pattern for descriptions
#
#     final_thumbnails = []
#     final_full_res_images = []
#     final_descriptions = []
#     print(type(matched_description))
#     print('made it')
#     if len(cleaned_urls) >= 10:
#         print('made it above 10')
#         final_image_urls = cleaned_urls[:10]
#         final_descriptions = matched_description[:10]
#         final_source_url = cleaned_source[:10]
#         return final_image_urls, final_descriptions, final_source_url
#     else:
#         print('made it below 10')
#         min_length = min(len(cleaned_urls), len(matched_description), len(cleaned_source))
#
#         print(f"{min_length}\nImg Urls: {len(cleaned_urls)}\nDescriptions: {len(matched_description)}\nSource Urls: {len(cleaned_source)}")
#         final_image_urls = cleaned_urls[:min_length]
#         final_descriptions = matched_description[:min_length]
#         final_source_url = cleaned_source[:min_length]
#         print(f"{min_length}\nImg Urls New: {len(final_image_urls)}\nDescriptions New: {len(final_descriptions)}\nSource Urls New: {len(final_source_url)}")
#         return final_image_urls, final_descriptions,final_source_url
#
#
#
def get_original_images(html_bytes):
    try:
        detected_encoding = chardet.detect(html_bytes)['encoding']
        soup = html_bytes.decode(detected_encoding)
    except Exception as e:
        print(e)
        soup = html_bytes.decode('utf-8')
    # with open ('soup.txt', 'w') as f:
    #     f.write(soup)


    #print(f"type: { type(soup)}")

    start_tag = 'FINANCE",[22,1]]]]]'
    #end_tag = ':[null,null,null,1,['
    end_tag = ':[null,null,null,"glbl'
    # with open('text.html', 'w', encoding='utf-8') as file:
    #      file.write(soup)
    matched_google_image_data = LR().get(soup, start_tag, end_tag)
    if 'Error' in matched_google_image_data:
        print('tags not found')
        return None
    if not matched_google_image_data:
        print('No matched_google_image_data')
        return (['No start_tag or end_tag'],['No start_tag or end_tag'],['No start_tag or end_tag'],['No start_tag or end_tag'])
    matched_google_image_data = str(matched_google_image_data).replace('\u003d','=')
    matched_google_image_data = str(matched_google_image_data).replace('\u0026', '&')

    # print(matched_google_image_data)
    # print(type(matched_google_image_data))

    # thumbnails = [
    #     bytes(bytes(thumbnail, 'utf-8').decode("unicode-escape"), "utf-8").decode("unicode-escape") for thumbnail in
    #     matched_google_image_data
    # ]
    # print(thumbnails)
    thumbnails = matched_google_image_data
    if '"2003":' not in thumbnails:
        print('No 2003 tag found')
        return (['No google image results found'],['No google image results found'],['No google image results found'],['No google image results found'])
    matched_google_images_thumbnails = ", ".join(
         re.findall(r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]',
                    str(thumbnails))).split(", ")

    regex_pattern_desc = r'"2003":\[null,"[^"]*","[^"]*","(.*?)"'
    # print(matched_google_images_thumbnails)
    matched_description = re.findall(regex_pattern_desc, str(thumbnails))

    regex_pattern_src = r'"2003":\[null,"[^"]*","(.*?)"'
    matched_source = re.findall(regex_pattern_src, str(thumbnails))
    #print(matched_source)
    removed_matched_google_images_thumbnails = re.sub(
        r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', "", str(thumbnails))

    # Extract full resolution images
    matched_google_full_resolution_images = re.findall(r"(?:|,),\[\"(https:|http.*?)\",\d+,\d+\]",
                                                       removed_matched_google_images_thumbnails)


    full_res_images = [
        bytes(bytes(img, "utf-8").decode("unicode-escape"), "utf-8").decode("unicode-escape") for img in
        matched_google_full_resolution_images
    ]
    cleaned_urls = [clean_image_url(url) for url in full_res_images]

    cleaned_source = [clean_source_url(url) for url in matched_source]
    cleaned_thumbs = [clean_source_url(url) for url in matched_google_images_thumbnails]
    if not cleaned_urls:
        cleaned_urls=cleaned_thumbs
    # print(len(cleaned_descriptions))
    # print(matched_description)
    # Assume descriptions are extracted
    # descriptions = LR().get(soup, '"2008":[null,"', '"]}],null,') # Replace 'description_pattern' with your actual regex pattern for descriptions

    final_thumbnails = []
    final_full_res_images = []
    final_descriptions = []
    # print(type(matched_description))
    # print('made it')
    if len(cleaned_urls) >= 8:
        print('made it above 10')
        
        if len(matched_description) <= 8:
            matched_description = matched_description+["No descriptions found"]*(8-len(matched_description))
        if len(cleaned_source) <= 8:
            cleaned_source = cleaned_source+["No sources found"]*(8-len(cleaned_source))
        if len(cleaned_thumbs) <= 8:
            cleaned_thumbs = cleaned_thumbs+["No thumbnails found"]*(8-len(cleaned_thumbs))


        final_image_urls = cleaned_urls[:8]
        final_descriptions = matched_description[:8]
        final_source_url = cleaned_source[:8]
        final_thumbs = cleaned_thumbs[:8]    
            
            
            
        #########    
        print(len(final_image_urls),len(final_descriptions),len(final_source_url),len(final_thumbs))
        max_length = max(len(final_image_urls),len(final_descriptions),len(final_source_url),len(final_thumbs))
        min_length = min(len(final_image_urls),len(final_descriptions),len(final_source_url),len(final_thumbs))
        if max_length != min_length:
            raise ValueError
        #################
        
        
        return final_image_urls, final_descriptions, final_source_url,final_thumbs
    else:
        print('made it below 10')
        min_length = len(cleaned_urls)
        print(f"Cleaned Urls: {len(cleaned_urls)}\n Matched Description: {len(matched_description)}\n Cleaned Sources: {len(cleaned_source)}")
        # print(f"{min_length}\nImg Urls: {len(cleaned_urls)}\nDescriptions: {len(matched_description)}\nSource Urls: {len(cleaned_source)}")
        
        
        
        if len(matched_description) <= min_length:
            matched_description = matched_description+["No descriptions found"]*(min_length - len(matched_description))
        if len(cleaned_source) <= min_length:
            cleaned_source = cleaned_source+["No sources found"]*(min_length - len(cleaned_source))
        if len(cleaned_thumbs) <= min_length:
            cleaned_thumbs = cleaned_thumbs+["No thumbnails found"]*(min_length - len(cleaned_thumbs))
            
            
            
   
        final_image_urls = cleaned_urls[:min_length]
        final_descriptions = matched_description[:min_length]
        final_source_url = cleaned_source[:min_length]
        final_thumbs = cleaned_thumbs[:min_length]
        
        print(len(final_image_urls),len(final_descriptions),len(final_source_url),len(final_thumbs))
        max_length = max(len(final_image_urls),len(final_descriptions),len(final_source_url),len(final_thumbs))
        min_length = min(len(final_image_urls),len(final_descriptions),len(final_source_url),len(final_thumbs))
        if max_length != min_length:
            raise ValueError
        # print(f"{min_length}\nImg Urls New: {len(final_image_urls)}\nDescriptions New: {len(final_descriptions)}\nSource Urls New: {len(final_source_url)}\nThumbs New: {len(final_thumbs)}")
        return final_image_urls, final_descriptions, final_source_url,final_thumbs
def clean_source_url(s):
    # First, remove '\\\\' to simplify handling
    simplified_str = s.replace('\\\\', '')

    # Mapping of encoded sequences to their decoded characters
    replacements = {
        'u0026': '&',
        'u003d': '=',
        'u003f': '?',
        'u0020': ' ',
        'u0025': '%',
        'u002b': '+',
        'u003c': '<',
        'u003e': '>',
        'u0023': '#',
        'u0024': '$',
        'u002f': '/',
        'u005c': '\\',
        'u007c': '|',
        'u002d': '-',
        'u003a': ':',
        'u003b': ';',
        'u002c': ',',
        'u002e': '.',
        'u0021': '!',
        'u0040': '@',
        'u005e': '^',
        'u0060': '`',
        'u007b': '{',
        'u007d': '}',
        'u005b': '[',
        'u005d': ']',
        'u002a': '*',
        'u0028': '(',
        'u0029': ')'
    }

    # Apply the replacements
    for encoded, decoded in replacements.items():
        simplified_str = simplified_str.replace(encoded, decoded)

    return simplified_str
def clean_image_url(url):
    # Pattern matches common image file extensions followed by a question mark and any characters after it
    pattern = re.compile(r'(.*\.(?:png|jpg|jpeg|gif))(?:\?.*)?', re.IGNORECASE)

    # Search for matches in the input URL
    match = pattern.match(url)

    # If a match is found, return the part of the URL before the query parameters (group 1)
    if match:
        return match.group(1)

    # If no match is found, return the original URL
    return url

# with open("text.html", "r", encoding='utf-8') as f:
#     html_content = f.read()
#     results = get_original_images(html_content)
#     print(results)