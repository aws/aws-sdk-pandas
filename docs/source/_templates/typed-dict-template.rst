{{ objname }}
{{ underline }}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
    :show-inheritance:

    {% block attributes_summary %}
    {% if attributes %}

    .. rubric:: Attributes

    .. autosummary::
    {% for item in attributes %}
        ~{{ name }}.{{ item }}
    {%- endfor %}

    {% endif %}
    {% endblock %}

    {% block methods_documentation %}
    {% if methods %}

    .. rubric:: Attributes Documentation

    {% for item in attributes %}
    .. autoattribute:: {{ item }}
    {%- endfor %}

    {% endif %}
    {% endblock %}
