package esl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_dataFilename(t *testing.T) {
	type args struct {
		path   string
		fileId uint16
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case 1",
			args: args{
				path:   "/tmp",
				fileId: 1,
			},
			want: "/tmp/0000000001.esld",
		},
		{
			name: "case 2",
			args: args{
				path:   "",
				fileId: 10,
			},
			want: "0000000010.esld",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, dataFilename(tt.args.path, tt.args.fileId), "dataFilename(%v, %v)", tt.args.path, tt.args.fileId)
		})
	}
}

func Test_hintFilename(t *testing.T) {
	type args struct {
		path   string
		fileId uint16
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case 1",
			args: args{
				path:   "/tmp",
				fileId: 1,
			},
			want: "/tmp/0000000001.hint",
		},
		{
			name: "case 2",
			args: args{
				path:   "",
				fileId: 10,
			},
			want: "0000000010.hint",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, hintFilename(tt.args.path, tt.args.fileId), "hintFilename(%v, %v)", tt.args.path, tt.args.fileId)
		})
	}
}

func Test_fileIdFromFilename(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name    string
		args    args
		want    uint16
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "case 1",
			args: args{
				filename: "/tmp/0000000001.esld",
			},
			want:    1,
			wantErr: assert.NoError,
		},
		{
			name: "case 2",
			args: args{
				filename: "0000000001.esld",
			},
			want:    1,
			wantErr: assert.NoError,
		},
		{
			name: "case 3",
			args: args{
				filename: "/tmp/0000000001.hint",
			},
			want:    1,
			wantErr: assert.NoError,
		},
		{
			name: "case 4",
			args: args{
				filename: "0000000001.hint",
			},
			want:    1,
			wantErr: assert.NoError,
		},
		{
			name: "case 5",
			args: args{
				filename: "/tmp/0000000001",
			},
			want:    0,
			wantErr: assert.Error,
		},
		{
			name: "case 6",
			args: args{
				filename: "abcd.hint",
			},
			want:    0,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fileIdFromFilename(tt.args.filename)
			if !tt.wantErr(t, err, fmt.Sprintf("fileIdFromFilename(%v)", tt.args.filename)) {
				return
			}
			assert.Equalf(t, tt.want, got, "fileIdFromFilename(%v)", tt.args.filename)
		})
	}
}

func Test_lastFileIdFromFilenames(t *testing.T) {
	type args struct {
		filenames []string
	}
	tests := []struct {
		name    string
		args    args
		want    uint16
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "case 1",
			args: args{
				filenames: []string{
					"/tmp/0000000001.esld",
					"/tmp/0000000002.esld",
					"/tmp/0000000003.esld",
				},
			},
			want:    3,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lastFileIdFromFilenames(tt.args.filenames)
			if !tt.wantErr(t, err, fmt.Sprintf("lastFileIdFromFilenames(%v)", tt.args.filenames)) {
				return
			}
			assert.Equalf(t, tt.want, got, "lastFileIdFromFilenames(%v)", tt.args.filenames)
		})
	}
}
