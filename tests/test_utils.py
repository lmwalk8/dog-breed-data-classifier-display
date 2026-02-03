import pytest

from app import breed_name_to_slug, slug_to_breed_name


@pytest.mark.utils
def test_breed_name_to_slug_simple():
    """Test converting simple breed name to slug."""
    assert breed_name_to_slug("Beagle") == "beagle"
    assert breed_name_to_slug("Golden Retriever") == "golden-retriever"


@pytest.mark.utils
def test_breed_name_to_slug_multiple_spaces():
    """Test converting breed name with multiple spaces to slug."""
    assert breed_name_to_slug("Border Collie") == "border-collie"
    assert (
        breed_name_to_slug("Cavalier King Charles Spaniel")
        == "cavalier-king-charles-spaniel"
    )


@pytest.mark.utils
def test_breed_name_to_slug_case_insensitive():
    """Test that breed name to slug conversion is case insensitive."""
    assert breed_name_to_slug("BEAGLE") == "beagle"
    assert breed_name_to_slug("BeAgLe") == "beagle"


@pytest.mark.utils
def test_slug_to_breed_name_simple():
    """Test converting simple slug back to breed name."""
    assert slug_to_breed_name("beagle") == "Beagle"
    assert slug_to_breed_name("golden-retriever") == "Golden Retriever"


@pytest.mark.utils
def test_slug_to_breed_name_multiple_hyphens():
    """Test converting slug with multiple hyphens back to breed name."""
    assert slug_to_breed_name("border-collie") == "Border Collie"
    assert (
        slug_to_breed_name("cavalier-king-charles-spaniel")
        == "Cavalier King Charles Spaniel"
    )


@pytest.mark.utils
def test_breed_name_slug_roundtrip():
    """Test that converting breed name to slug and back preserves the name (with title case)."""
    test_cases = [
        "Beagle",
        "Golden Retriever",
        "Border Collie",
        "Cavalier King Charles Spaniel",
        "German Shepherd",
    ]

    for breed_name in test_cases:
        slug = breed_name_to_slug(breed_name)
        converted_back = slug_to_breed_name(slug)
        assert converted_back == breed_name, f"Roundtrip failed for {breed_name}"
