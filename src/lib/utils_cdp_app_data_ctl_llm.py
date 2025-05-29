
import  vertexai
from    vertexai.generative_models import GenerativeModel
from    vertexai.preview.language_models import TextGenerationModel

def call_gemini(ctx, prompt: str):

    PROJECT_ID = "sitpub-customer-data-platform"
    vertexai.init(project=PROJECT_ID, location="us-central1")

    # set LLM Model
    model = GenerativeModel("gemini-1.5-flash-002")
    # prompt should be passed in, just call gemini with constructed prompt
    ctx.obj.logger.info("Prompt:")
    ctx.obj.logger.info(prompt)
    # call LLM
    llm_response = model.generate_content(prompt)
    # get response text
    llm_response_text = llm_response.text

    # Remove first and last lines from llm_response_text
    llm_response_lines = llm_response_text.split('\n')
    llm_response_text = '\n'.join(llm_response_lines[1:-2]) 
    #print(llm_response_text)
 
    return(llm_response_text)

#def call_gemini_llm_updated(ctx, prompt: str):
#
#    PROJECT_ID = "sitpub-customer-data-platform"
#    vertexai.init(project=PROJECT_ID, location="us-central1")
#
#    # Initialize the TextGenerationModel
#    model = TextGenerationModel.from_pretrained("text-bison@001")
#
#    # Log the prompt
#    ctx.obj.logger.info("Prompt:")
#    ctx.obj.logger.info(prompt)
#
#    # Call the LLM with the prompt
#    llm_response = model.predict(prompt, temperature=0.2, max_output_tokens=1024, top_k=40, top_p=0.8)
#
#    # Get response text
#    llm_response_text = llm_response.text
#
#    # Remove first and last lines from llm_response_text
#    llm_response_lines = llm_response_text.split('\n')
#    llm_response_text = '\n'.join(llm_response_lines[1:-2])
#
#    return llm_response_text