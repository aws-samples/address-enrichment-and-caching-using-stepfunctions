from functions.gather import app


def test_detect_language():
    input_payload = {}


    data = app.lambda_handler(input_payload, "")

    assert "Payload" in data
    assert "Shards" in data['Payload']
