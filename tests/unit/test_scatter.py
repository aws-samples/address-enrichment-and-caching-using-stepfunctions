from functions.scatter import app


def test_scatter():
    input_payload = {}


    data = app.lambda_handler(input_payload, "")

    assert "Payload" in data
    assert "Shards" in data['Payload']
