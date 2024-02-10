from flask import Flask, request, jsonify
import uuid
import subprocess
import os
import json
import boto3



JOBS_FILE='jobs_data.json'

def save_jobs():
    with open(JOBS_FILE, 'w') as file:
        json.dump(jobs, file)

def load_jobs():
    if os.path.exists(JOBS_FILE):
        with open(JOBS_FILE, 'r') as file:
            try:
                return json.load(file)
            except json.JSONDecodeError:
                return {} 
    return {}


app = Flask(__name__)
jobs = load_jobs()

@app.route('/annotations', methods=['GET'])
# This method is called when a user visits "/" on your server
def submit_annotation_job():
  try:
    bucket_name = request.args.get('bucket')
    key = request.args.get('key')

    if not bucket_name or not key:
        return jsonify({'code': 400, 'status': 'error', 'message': 'Missing bucket name or key'}), 400


    input_file_name = key.split('~')[-1]
    job_id = str(uuid.uuid4())

    #download files from s3
    home_dir = os.path.expanduser('~/mpcs-cc')
    data_dir = os.path.join(home_dir, 'anntools', 'download')
    os.makedirs(data_dir, exist_ok=True)  # Ensure the directory exists
    local_file_path = os.path.join(data_dir, input_file_name)
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, key, local_file_path)


    #track the job status
    job_dir = os.path.join(home_dir, 'jobs')
    os.makedirs(job_dir, exist_ok=True)
    job_file_path = os.path.join(job_dir, f"{job_id}.job")
    with open(job_file_path, 'w') as file:
        file.write(f"Input file: {input_file_name}\nStatus: Running")


    subprocess.Popen(['python', 'anntools/hw3_run.py', 'anntools/download/'+input_file_name])

    return jsonify({'code': 201, 'data': {'job_id': job_id, 'input_file': input_file_name}}), 201
  except Exception as e:
    return jsonify({'code': 500, 'status': 'error', 'message': str(e)}), 500



@app.route('/annotations/<job_id>', methods=['GET'])
def get_job(job_id):
  try:
      home_dir = os.path.expanduser('~/mpcs-cc')
      job_dir = os.path.join(home_dir, 'jobs')
      job_file_path = os.path.join(job_dir, f"{job_id}.job")

      if not os.path.exists(job_file_path):
          return jsonify({'code': 404, 'status': 'error', 'message': 'Job not found'}), 404

      with open(job_file_path, 'r') as file:
          job_info = file.read()

      input_file = jobs.get(job_id)
      if not input_file:
          return jsonify({'code': 404, 'status': 'error', 'message': 'Input file not found'}), 404

      log_file_path = os.path.join(home_dir, 'anntools', 'data', f"{input_file}.count.log")

      if os.path.exists(log_file_path):
          with open(log_file_path, 'r') as log_file:
            log_content = log_file.read()
          with open(job_file_path, 'w') as file:
            file.write(f"Input file: {input_file}\nStatus: Completed")
          return jsonify({'code': 200, 'data': {'job_id': job_id, 'job_status': 'completed', 'log': log_content}}), 200
      else:
          return jsonify({'code': 200, 'data': {'job_id': job_id, 'job_status': 'running'}}), 200
  except Exception as e:
        return jsonify({'code': 500, 'status': 'error', 'message': str(e)}), 500


# @app.route('/annotations', methods=['GET'])
# def retrieve_job_list():
#     try:
#         home_dir = os.path.expanduser('~/mpcs-cc')
#         job_dir = os.path.join(home_dir, 'jobs')
#         job_files = [f for f in os.listdir(job_dir) if f.endswith('.job')]

#         jobs_list = []
#         for job_file in job_files:
#             job_id = job_file.split('.')[0]
#             job_detail_url = f"http://yanze41-hw3-ann.mpcs-cc.com:5000/annotations/{job_id}"
#             jobs_list.append({'job_id':job_id, 'job_detail':job_detail_url})
#         return jsonify({"code": 200,
#          "data": {
#            "jobs":jobs_list}}),200
#     except Exception as e:
#         return jsonify({'code': 500, 'status': 'error', 'message': str(e)}), 500



# Run the app server
app.run(host='0.0.0.0', debug=True)