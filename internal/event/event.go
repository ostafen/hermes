package event

type Metadata map[string]string

const (
	MetadataKeyEventType = "type"
)

func (m Metadata) EventType() string {
	return m[MetadataKeyEventType]
}

type ContentType string

const (
	ContentTypeJson ContentType = "application/json"
)

type EventData struct {
	EventID     string `json:"eventId"`
	ContentType ContentType
	Metadata    Metadata `json:"metadata"`
	Data        any      `json:"data"`
}

func (data *EventData) IsJson() bool {
	return data.ContentType == ContentTypeJson
}
