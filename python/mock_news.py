def get_mock_response():
    import random
    rId1 = str(random.randint(1,100000))
    rId2 = str(random.randint(1,100000))

    mock_news_response = {
        "news": [
            {
                "id": rId1,
                "title": "Obama gives a speech on climate change",
                "description": "Former president Obama emphasized the importance of clean energy.",
                "url": "https://example.com/obama-speech",
                "author": "Jane Doe",
                "published": "2025-11-21T12:00:00Z"
            },
            {
                "id": rId2,
                "title": "Obama visits local school",
                "description": "Obama visited a school to promote education initiatives.",
                "url": "https://example.com/obama-school",
                "author": "John Smith",
                "published": "2025-11-20T09:30:00Z"
            }
        ]
    }
    return mock_news_response
