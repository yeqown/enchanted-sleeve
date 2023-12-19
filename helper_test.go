package esl

import (
	"fmt"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{
			name: "case 2",
			args: args{
				filenames: []string{
					"/tmp/0000000001.esld",
					"/tmp/0000000001.hint",
					"/tmp/0000000002.esld",
					"/tmp/0000000002.hint",
				},
			},
			want:    2,
			wantErr: assert.NoError,
		},
		{
			name: "case 3 only 1 file",
			args: args{
				filenames: []string{
					"/tmp/0000000001.esld",
				},
			},
			want:    1,
			wantErr: assert.NoError,
		},
		{
			name: "case 4 no file",
			args: args{
				filenames: []string{},
			},
			want:    0,
			wantErr: assert.NoError,
		},
		{
			name: "case 5 not ordered files",
			args: args{
				filenames: []string{
					"/tmp/0000000003.esld",
					"/tmp/0000000001.esld",
					"/tmp/0000000004.esld",
					"/tmp/0000000002.esld",
				},
			},
			want:    4,
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

func Test_ensurePath(t *testing.T) {
	existPath := "/tmp/exist"
	nonExistsPath := "/tmp/exist/non-exist"

	memMapFs := afero.NewMemMapFs()
	err := memMapFs.MkdirAll(existPath, 0755)
	require.NoError(t, err)

	// TODO: mock mkdir failed error case
	// memMapFs2 := afero.NewMemMapFs()
	// // set permission to a value which will cause child 0744 dir creation to fail
	// err = memMapFs.MkdirAll(existPath, 0000)
	// require.NoError(t, err)

	type args struct {
		fs   FileSystem
		path string
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "case 1, exist path, just return",
			args: args{
				fs:   memMapFs,
				path: existPath,
			},
			wantErr: assert.NoError,
		},
		{
			name: "case 2, non exist path, create success",
			args: args{
				fs:   memMapFs,
				path: nonExistsPath,
			},
			wantErr: assert.NoError,
		},
		// TODO: mock mkdir failed error case
		// {
		// 	name: "case 3, non exist path, create fail",
		// 	args: args{
		// 		fs:   memMapFs2,
		// 		path: nonExistsPath,
		// 	},
		// 	wantErr: assert.Error,
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, ensurePath(tt.args.fs, tt.args.path), fmt.Sprintf("ensurePath(%v, %v)", tt.args.fs, tt.args.path))
		})
	}
}

func Test_takeDBPathSnap_hasHintFiles(t *testing.T) {
	memMapFs := afero.NewMemMapFs()
	err := memMapFs.MkdirAll("/tmp", 0755)
	require.NoError(t, err)
	_, err = memMapFs.Create("/tmp/0000000001.esld")
	require.NoError(t, err)
	_, err = memMapFs.Create("/tmp/0000000002.esld")
	require.NoError(t, err)
	_, err = memMapFs.Create("/tmp/0000000003.xxx")
	require.NoError(t, err)
	_, err = memMapFs.Create("/tmp/0000000001.hint")
	require.NoError(t, err)
	_, err = memMapFs.Create("/tmp/0000000002.hint")
	require.NoError(t, err)

	dbPathSnap, err := takeDBPathSnap(memMapFs, "/tmp")
	require.NoError(t, err)
	assert.Equal(t, "/tmp", dbPathSnap.path)
	assert.Equal(t, 2, len(dbPathSnap.dataFiles))
	assert.Equal(t, 2, len(dbPathSnap.hintFiles))
	assert.Equal(t, uint16(3), dbPathSnap.lastDataFileId)
	assert.Equal(t, "/tmp/0000000003.esld", dbPathSnap.lastActiveFile("/tmp"))
}

func Test_takeDBPathSnap_noHintFiles(t *testing.T) {
	memMapFs := afero.NewMemMapFs()
	err := memMapFs.MkdirAll("/tmp", 0755)
	require.NoError(t, err)
	_, err = memMapFs.Create("/tmp/0000000001.esld")
	require.NoError(t, err)
	_, err = memMapFs.Create("/tmp/0000000002.esld")
	require.NoError(t, err)
	_, err = memMapFs.Create("/tmp/0000000003.xxx")
	require.NoError(t, err)

	dbPathSnap, err := takeDBPathSnap(memMapFs, "/tmp")
	require.NoError(t, err)
	assert.Equal(t, "/tmp", dbPathSnap.path)
	assert.Equal(t, 2, len(dbPathSnap.dataFiles))
	assert.Equal(t, 0, len(dbPathSnap.hintFiles))
	assert.Equal(t, uint16(2), dbPathSnap.lastDataFileId)
	assert.Equal(t, "/tmp/0000000002.esld", dbPathSnap.lastActiveFile("/tmp"))
}

func Test_backupFile_backup(t *testing.T) {
	fs := afero.NewMemMapFs()
	filename := "/tmp/0000000001.esld"
	_, err := fs.Create(filename)
	require.NoError(t, err)
	exist, err := afero.Exists(fs, filename)
	require.NoError(t, err)
	assert.True(t, exist)

	restore, clean, err := backupFile(fs, filename)
	require.NoError(t, err)
	assert.NotNil(t, restore)
	assert.NotNil(t, clean)

	// since it rename to /tmp/0000000001.esld.bak, so it should not exist
	exist, err = afero.Exists(fs, filename)
	require.NoError(t, err)
	assert.False(t, exist)
}

func Test_backupFile_clean(t *testing.T) {
	fs := afero.NewMemMapFs()
	filename := "/tmp/0000000001.esld"
	_, err := fs.Create(filename)
	require.NoError(t, err)
	exist, err := afero.Exists(fs, filename)
	require.NoError(t, err)
	assert.True(t, exist)

	restore, clean, err := backupFile(fs, filename)
	require.NoError(t, err)
	assert.NotNil(t, restore)
	assert.NotNil(t, clean)

	err = clean()
	require.NoError(t, err)

	exist, err = afero.Exists(fs, filename)
	require.NoError(t, err)
	assert.False(t, exist)
}

func Test_backupFile_restore(t *testing.T) {
	fs := afero.NewMemMapFs()
	filename := "/tmp/0000000001.esld"
	_, err := fs.Create(filename)
	require.NoError(t, err)
	exist, err := afero.Exists(fs, filename)
	require.NoError(t, err)
	assert.True(t, exist)

	restore, clean, err := backupFile(fs, filename)
	require.NoError(t, err)
	assert.NotNil(t, restore)
	assert.NotNil(t, clean)

	err = restore()
	require.NoError(t, err)

	exist, err = afero.Exists(fs, filename)
	require.NoError(t, err)
	assert.True(t, exist)
}
