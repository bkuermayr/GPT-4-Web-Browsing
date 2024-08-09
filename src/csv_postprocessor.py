import numpy as np
import pandas as pd
import requests
from ftp_client import upload_file


def process_data(csv_feed_url, remote_directory, output_file_name):
    # Download the CSV file
    response = requests.get(csv_feed_url)
    local_file_name = csv_feed_url.split('/')[-1]
    with open(local_file_name, 'wb') as file:
        file.write(response.content)

    # Read the CSV file, parsing all columns as string
    df = pd.read_csv(local_file_name, sep='|', dtype=str, encoding='utf-8', engine='python')

    # define option string as constants
    eisen = "name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=-2 inch$#is_default=0|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=-1,5 inch$#is_default=0|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=-1 inch$#is_default=0|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=-0,5 inch$#is_default=0|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Standard$#is_default=1|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=+0,5 inch$#is_default=0|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=+1 inch$#is_default=0|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=+1,5 inch$#is_default=0|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=+2 inch$#is_default=0|name=Liewinkel$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=2° Flat$#is_default=0|name=Liewinkel$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=1° Flat$#is_default=0|name=Liewinkel$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Standard$#is_default=1|name=Liewinkel$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=1° Up$#is_default=0|name=Liewinkel$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=2° Up$#is_default=0|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Standard$#is_default=1|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Standard +1 Tape$#is_default=0|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Standard +2 Tapes$#is_default=0|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Midsize$#is_default=0|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Midsize + 1 Tape$#is_default=0|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Midsize + 2 Tapes$#is_default=0|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Jumbo$#is_default=0|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Jumbo +1 Tape$#is_default=0"
    holz = "name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Standard$#is_default=1|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Standard +1 Tape$#is_default=|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Standard +2 Tapes$#is_default=|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Midsize$#is_default=|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Midsize + 1 Tape$#is_default=|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Midsize + 2 Tapes$#is_default=|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Jumbo$#is_default=|name=Griffstärke$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Jumbo +1 Tape$#is_default=|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=-1 inch$#is_default=|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=-0,5inch$#is_default=|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=Standard$#is_default=1|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=+0,5 inch$#is_default=|name=Schlägerlänge$#type=drop_down$#required=1$#price=0.000000$#sku=$#max_characters=0$#file_extension=$#image_size_x=0$#image_size_y=0$#price_type=fixed$#option_title=+ 1 inch$#is_default="

    def custom_fitting_option(row):
        if(row['Product Type (Magento)'] == 'simple'):
            return ""
        if row['Fittingoption'] == 'Holz':
            return holz
        elif row['Fittingoption'] == 'Eisen':
            return eisen
        return ""

    # Process columns
    df['Geschlecht'] = df['Geschlecht'].replace('Männlich', 'Herren').replace('Weiblich', 'Damen')
    df['Geschlecht Hauptprodukt'] = df['Geschlecht Hauptprodukt'].replace('Männlich', 'Herren').replace('Weiblich', 'Damen')
    df['Configurable Variations (Magento)'] = df['Configurable Variations (Magento)'].replace('Männlich', 'Herren').replace('Weiblich', 'Damen')

    df['Marken'] = df['Marken'].astype(str)
    df['Product Model'] = df['Product Model'].astype(str)
    df['Geschlecht'] = df['Geschlecht'].astype(str).replace('nan', np.nan)

    df['Product Model'] = df.apply(lambda row: row['Marken'] + " " + row['Product Model'] if pd.notna(row['Marken']) and row['Marken'] != 'nan' and row['Marken'] not in row['Product Model'] else row['Product Model'], axis=1)
    df['Product Model'] = df['Product Model'].replace({" HE ": " Herren ", " DA ": " Damen "}, regex=True)
    df['Product Model'] = df.apply(lambda row: row['Product Model'] + " " + row['Geschlecht Hauptprodukt'] if pd.notna(row['Geschlecht Hauptprodukt']) and row['Geschlecht Hauptprodukt'] != 'nan' and row['Geschlecht Hauptprodukt'] not in row['Product Model'] and row['Geschlecht Hauptprodukt'] != 'Unisex' else row['Product Model'], axis=1)
    
    parent_geschlecht = df.groupby('Parent SKU')['Geschlecht'].apply(lambda x: x.dropna().unique()[0] if len(x.dropna().unique()) == 1 else np.nan)
    df['parent_geschlecht'] = df['Parent SKU'].map(parent_geschlecht)
    df['Lagerstand Hauptprodukt'] = df['Lagerstand Hauptprodukt'].replace('Ja', '10')

    # Create an empty DataFrame to store deleted products
    deleted_products = pd.DataFrame()

    # Exclude all without EAN number if is variant product (Parent SKU is not empty)
    mask_ean = df['ean'].isna() & (df['Product Type (Magento)'] == 'simple')
    deleted_products = pd.concat([deleted_products, df[mask_ean]], ignore_index=True)
    df = df[~mask_ean]

    # Exclude all simple products without UVP
    mask_uvp = df['UVP'].isna() & (df['Product Type (Magento)'] == 'simple')
    deleted_products = pd.concat([deleted_products, df[mask_uvp]], ignore_index=True)
    df = df[~mask_uvp]

    # filter out custom products
    mask = ~((df['sku'].apply(lambda x: "CUSTOM" in str(x).upper())) | 
             (df['Flex'].apply(lambda x: "CUSTOM" in str(x))) | 
             (df['Schaft'].apply(lambda x: "CUSTOM" in str(x))))

    deleted_products = pd.concat([deleted_products, df[~mask]], ignore_index=True)
    df = df[mask]

    # add all products with "Online ausschließen" = "Ja" to deleted_products
    deleted_products = pd.concat([deleted_products, df[df['Online ausschließen'] == 'Ja']], ignore_index=True)
    df = df[df['Online ausschließen'] != 'Ja']

    # Create a new column "Fitting Option" based on the values in the row
    df['custom_options'] = df.apply(custom_fitting_option, axis=1)

    # add status "enabled" / "disabled" based on whether product has an image
    df.loc[(df['Produktbilder'].notna()) & (df['Product Type (Magento)'] == 'configurable'), 'Status (Magento)'] = "1"
    df.loc[(df['Produktbilder'].isna()) & (df['Product Type (Magento)'] == 'configurable'), 'Status (Magento)'] = "2"

    # Create a list of all SKUs that appear in the 'Parent SKU' column
    parent_skus = df['Parent SKU'].dropna().unique()
    df = df[~((df['Product Type (Magento)'] == 'configurable') & (~df['sku'].isin(parent_skus)))]

    df['Schuhgröße EU'] = df['Schuhgröße EU'].str.replace('$#', ',')
    df['Angebotspreis GSS'] = df['Angebotspreis GSS'].str.replace('EUR', '')

    # log and count how many products have no image
    print('Products without image:', len(df[df['Base Product Image'].isna() & (df['Product Type (Magento)'] == 'configurable')]))

    df.loc[df['Product Type (Magento)'] == 'configurable', 'Additional Attributes (Magento)'] = 'has_options=1$#required_options=1$#' + df['Additional Attributes (Magento)']    

    df['image_name'] = df['sku'].apply(lambda x: 'P_' + x.replace('--', '') + '.jpg' if x.startswith('--') and x.endswith('--') else x + '.jpg')
    replace_dict = {'Ä': 'AE', 'Ö': 'OE', 'Ü': 'UE'}
    df['image_name'] = df['image_name'].replace(replace_dict, regex=True)

    images = df
    images = images.dropna(subset=['image_name'])
    df['image_name'] = df['sku'].map(images.set_index('sku')['image_name']).str.replace('.jpg', '')

    df['image_count'] = df.apply(lambda row: 0 if (pd.isna(row['Base Product Image']) or row['Base Product Image'] == '') and (pd.isna(row['Additional Product Images']) or row['Additional Product Images'] == '') else (1 if pd.notna(row['Base Product Image']) and row['Base Product Image'] != '' else 0) + (len(row['Additional Product Images'].split('$#')) if pd.notna(row['Additional Product Images']) and row['Additional Product Images'] != '' else 0), axis=1)
    df['Base Product Image File (Magento)'] = df['image_name'].apply(lambda x: f"{x}_1.jpg" if pd.notna(x) and x != "" else "")
    df['Additional Images File (Magento)'] = df.apply(lambda row: '$#'.join([f"{row['image_name']}_{i}.jpg" for i in range(2, int(row['image_count'] + 1))]) if pd.notna(row['image_name']) and row['image_name'] != "" else "", axis=1)

    def filter_variations_by_sku(row):
        variations = row['Configurable Variations (Magento)']
        if pd.isna(variations):
            return ''
        result_variations = []
        for variation in variations.split('|'):
            sku = next((part.split('=')[1] for part in variation.split('$#') if part.startswith('sku=')), None)
            if sku not in deleted_products['sku'].values and "CUSTOM" not in sku.upper() and "CUSTOM" not in str(row['Flex']) and "CUSTOM" not in str(row['Schaft']):
                result_variations.append(variation)
        return '|'.join(result_variations)

    df['Schläger Kombination'] = df['Schläger Kombination'].str.replace('$#', ', ')

    def fix_komplettsets(row):
        if isinstance(row['Additional Attributes (Magento)'], str):
            attributes = row['Additional Attributes (Magento)'].split('$#')
            attributes = ['schlaeger_kombination_1454=' + str(row['Schläger Kombination']) if attr.startswith('schlaeger_kombination_1454=') else attr for attr in attributes]
            row['Additional Attributes (Magento)'] = '$#'.join(attributes)
        if isinstance(row['Configurable Variations (Magento)'], str):
            variations = row['Configurable Variations (Magento)'].split('|')
            updated_variations = []
            for var in variations:
                parts = var.split('$#')
                sku = next((part.split('=')[1] for part in parts if part.startswith('sku=')), None)
                if sku is not None:
                    matching_rows = df[df['sku'] == sku]
                    if not matching_rows.empty:
                        sku_row = matching_rows.iloc[0]
                        parts = [part if not part.startswith('schlaeger_kombination_1454=') else 'schlaeger_kombination_1454=' + str(sku_row['Schläger Kombination']) for part in parts]
                updated_variations.append('$#'.join(parts))
            row['Configurable Variations (Magento)'] = '|'.join(updated_variations)   
        return row

    df = df.apply(fix_komplettsets, axis=1)
    df['Configurable Variations (Magento)'] = df.apply(filter_variations_by_sku, axis=1)

    def remove_attribute(variations, attribute, condition):
        if pd.isna(variations):
            return ''
        variations = variations.split('|')
        for i in range(len(variations)):
            attributes = variations[i].split('$#')
            if any(attr.startswith(condition + '=') for attr in attributes):
                attributes = [attr for attr in attributes if not attr.startswith(attribute + '=')]
            variations[i] = '$#'.join(attributes)
        return '|'.join(variations)

    df['Configurable Variations (Magento)'] = df['Configurable Variations (Magento)'].apply(lambda x: remove_attribute(x, 'lieferantenfarbe_1420', 'farbe_de'))
    df['Configurable Variations (Magento)'] = df['Configurable Variations (Magento)'].apply(lambda x: remove_attribute(x, 'loft_1430', 'schlaegernummer_1441'))
    df['Configurable Variations (Magento)'] = df['Configurable Variations (Magento)'].apply(lambda x: remove_attribute(x, 'schaft_1433', 'schlaeger_schaft_material'))

    def remove_attribute_additional(attributes, attribute, condition):
        if pd.isna(attributes):
            return ''
        attributes = attributes.split('$#')
        if any(attr.startswith(condition + '=') for attr in attributes):
            attributes = [attr for attr in attributes if not attr.startswith(attribute + '=')]
        return '$#'.join(attributes)

    df['Additional Attributes (Magento)'] = df['Additional Attributes (Magento)'].apply(lambda x: remove_attribute_additional(x, 'lieferantenfarbe_1420', 'farbe_de'))
    df['Additional Attributes (Magento)'] = df['Additional Attributes (Magento)'].apply(lambda x: remove_attribute_additional(x, 'loft_1430', 'schlaegernummer_1441'))
    df['Additional Attributes (Magento)'] = df['Additional Attributes (Magento)'].apply(lambda x: remove_attribute_additional(x, 'schaft_1433', 'schlaeger_schaft_material'))

    def remove_attribute_condition(row, column, attribute, condition):
        if row[condition] == 'Textil':
            if pd.notna(row[column]):
                attributes = row[column].split('$#')
                attributes = [attr for attr in attributes if not attr.startswith(attribute + '=')]
                row[column] = '$#'.join(attributes)
        return row

    df = df.apply(lambda x: remove_attribute_condition(x, 'Additional Attributes (Magento)', 'hand_1432', 'Most General Category'), axis=1)
    df = df.apply(lambda x: remove_attribute_condition(x, 'Configurable Variations (Magento)', 'hand_1432', 'Most General Category'), axis=1)

    df['Configurable Variations (Magento)'] = df['Configurable Variations (Magento)'].str.replace('lieferantenfarbe_1420', 'farbe_de')
    df['Additional Attributes (Magento)'] = df['Additional Attributes (Magento)'].str.replace('lieferantenfarbe_1420', 'farbe_de')

    def get_variation_attribute_names(variations):
        if pd.isna(variations):
            return ''
        variations = variations.split('|')
        variation_attribute_names = []
        for variation in variations:
            attributes = variation.split('$#')
            attribute_names = [attribute.split('=')[0] for attribute in attributes]
            variation_attribute_names.append(' - '.join(attribute_names))
        variation_attribute_names = list(set(variation_attribute_names))
        return ', '.join(variation_attribute_names)

    temp_df = df[df['Product Type (Magento)'] == 'configurable'].copy()
    temp_df['conf_variations'] = temp_df['Configurable Variations (Magento)'].apply(get_variation_attribute_names)
    invalid_parents = temp_df[temp_df['conf_variations'].apply(lambda x: len(set(x.split(', '))) > 1)]
    invalid_rows = df[df['sku'].isin(invalid_parents['sku']) | df['Parent SKU'].isin(invalid_parents['sku'])]
    invalid_rows = invalid_rows.copy()
    invalid_rows['conf_variations'] = invalid_rows['sku'].map(temp_df.set_index('sku')['conf_variations'])
    invalid_rows.to_csv('invalid_products.csv', sep='|', index=False)
    df = df[~df['sku'].isin(invalid_rows['sku'])]

    df.to_csv(output_file_name, sep='|', index=False)
    # Upload the file to the FTP server
    full_remote_path = remote_directory + output_file_name if remote_directory.endswith('/') else remote_directory + '/' + output_file_name
    upload_file(output_file_name, full_remote_path)


if __name__ == "__main__":
    process_data('https://oarreivvqvbvowbekecs.supabase.co/storage/v1/object/public/feeds/9884179a-a650-42b2-8f83-1ce58c97d84f.csv', '/chroot/home/ae2932a1/8f93964a1a.nxcli.io/pub/media/importexport/m/a/','magento_feed_1_1.csv')
