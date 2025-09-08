from etl.main import load_json

def test_load_json_accepts_array_and_object(tmp_path):
    arr = tmp_path/"arr.json"; arr.write_text('[{"a":1},{"a":2}]', encoding="utf-8")
    obj = tmp_path/"obj.json"; obj.write_text('{"a":1}', encoding="utf-8")
    assert len(load_json(str(arr))) == 2
    assert len(load_json(str(obj))) == 1