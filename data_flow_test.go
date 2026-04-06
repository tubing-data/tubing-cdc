package tubing_cdc

import "testing"

func TestTableIncludeRegex(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "dem_test", input: "cdc_test.dem_test", want: `cdc_test\.dem_test`},
		{name: "simple names", input: "db.t", want: `db\.t`},
		{name: "table name contains dot", input: "a.b.c", want: `a\.b\.c`},
		{name: "no dot", input: "invalid", wantErr: true},
		{name: "empty db", input: ".t", wantErr: true},
		{name: "empty table", input: "db.", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tableIncludeRegex(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}
