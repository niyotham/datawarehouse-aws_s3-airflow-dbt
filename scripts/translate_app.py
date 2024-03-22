import sys, os
from dotenv import dotenv_values, load_dotenv
import boto3
import pandas as pd
import logging
import warnings

warnings.simplefilter(action='ignore', category=Warning)

logger = logging.getLogger(__name__)

load_dotenv()
sys.path.append("../")
region_name= os.getenv('region_name')
aws_access_key_id = os.getenv('aws_access_key_id') 
aws_secret_access_key = os.getenv('aws_secret_access_key')


def text_translate(region_name,
                    aws_access_key_id,
                    aws_secret_access_key,
                    message,
                    lang_targeted,
                    code='auto'
                   ):
    """ Function to get aws translate client and translate the text"""
    try:
        translate = boto3.client('translate',
                            region_name= region_name,
                            aws_access_key_id = aws_access_key_id ,
                            aws_secret_access_key = aws_secret_access_key )
        translated_text=translate.translate_text(
                        Text= message,
                        SourceLanguageCode= code,
                        TargetLanguageCode= lang_targeted
                    )['TranslatedText']
        
    except Exception as e:
        logger.exception("Couldn't translate the text because of  %s.", e)
        raise
    else:
        return translate,translated_text
        

def create_comprehend(region_name,
                      aws_access_key_id,
                      aws_secret_access_key,
                      text,
                      lang_code='en'
                      ):
    """ Function to get aws comprehend client and get sentiments"""
    try:
        comprehend = boto3.client('comprehend',
                            region_name= region_name,
                            aws_access_key_id = aws_access_key_id ,
                            aws_secret_access_key = aws_secret_access_key )
        sentiment=comprehend.detect_sentiment(
                        Text= text,
                        LanguageCode=lang_code
                    )["Sentiment"]
        # logger.info("translate the text from  %s. ", lang_code)
    except TypeError as e:
        print("Couldn't translate the text becuase of error.")
        
    else:
        return comprehend,sentiment
    
def get_sentiments_df(df:pd.DataFrame, column, df_name, languageCode='en' ):
    """
    - column: column containing the text to analyse its sentiment
    """
    try:
        for index, row in df.iterrows():
        # Get the translated_desc into a variable
            description = str( df.loc[index, column])
            if description != '' or description != "nan":
                # Get the detect_sentiment response
                response = comprehend.detect_sentiment(
                Text=description, 
                LanguageCode=languageCode)
                # Get the sentiment key value into sentiment column
                df.loc[index, 'sentiment'] = response['Sentiment']
        print("success")
        df.to_csv(f'../data/{df_name}_sentiment.csv')
        return df
    except Exception as e:
        print('cannotget semtiment because of %s', e)



if __name__ == "__main__":

    message= "j'aime tellement mes frères et sœurs."
    translate,translated_text=text_translate(region_name,
                    aws_access_key_id,
                    aws_secret_access_key,
                    message,
                    'es',
                    code='auto')
    print(translated_text)

    comprehend,sentiment=create_comprehend(region_name,
                      aws_access_key_id,
                      aws_secret_access_key,
                      message,
                      lang_code='en'
                      )
    print(sentiment)

    df =pd.read_csv("../data/df_text_dumping.csv")
    df_1=df[:100]
    df_10=get_sentiments_df(df_1,'public_description',df_name='text_dumping')

    print(df_10.head())
    