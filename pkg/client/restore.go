package client

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/client/js"
)

type RestoreCmd struct {
	File string `default:"backup.tar.gz"`
}

func (c *Client) Restore(ctx context.Context, file string) error {

	tf, err := os.Open(file)
	if err != nil {
		return err
	}
	defer tf.Close()

	gz, err := gzip.NewReader(tf)
	if err != nil {
		return err
	}
	defer gz.Close()

	tr := tar.NewReader(gz)

	var stop bool
	for {
		header, err := tr.Next()

		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		}

		if header == nil || header.Typeflag != tar.TypeReg {
			continue
		}

		name := path.Clean(header.Name)
		switch name {
		case "object_types.json":
			if err := c.loadObjectTypes(ctx, tr); err != nil {
				return err
			}

		case "permissions.json":
			if err := c.loadPermissions(ctx, tr); err != nil {
				return err
			}

		case "relation_types.json":
			if err := c.loadRelationTypes(ctx, tr); err != nil {
				return err
			}

		case "objects.json":
			if err := c.loadObjects(ctx, tr); err != nil {
				return err
			}

		case "relations.json":
			if err := c.loadRelations(ctx, tr); err != nil {
				return err
			}

		default:
			stop = true
		}

		if stop {
			break
		}
	}

	return nil
}

func (c *Client) loadObjectTypes(ctx context.Context, r io.Reader) error {
	objTypes, _ := js.NewArrayReader(r)
	defer objTypes.Close()

	var m dsc.ObjectType
	for {
		err := objTypes.Read(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		resp, err := c.Writer.SetObjectType(ctx, &dsw.SetObjectTypeRequest{
			ObjectType: &m,
		})
		if err != nil {
			return err
		}

		fmt.Println("object_type", resp.Result.Id)
	}
	return nil
}

func (c *Client) loadPermissions(ctx context.Context, r io.Reader) error {
	permissions, _ := js.NewArrayReader(r)
	defer permissions.Close()

	var m dsc.Permission
	for {
		err := permissions.Read(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		resp, err := c.Writer.SetPermission(ctx, &dsw.SetPermissionRequest{
			Permission: &m,
		})
		if err != nil {
			return err
		}

		fmt.Println("permission", resp.Result.Id)
	}
	return nil
}

func (c *Client) loadRelationTypes(ctx context.Context, r io.Reader) error {
	relTypes, _ := js.NewArrayReader(r)
	defer relTypes.Close()

	var m dsc.RelationType
	for {
		err := relTypes.Read(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		resp, err := c.Writer.SetRelationType(ctx, &dsw.SetRelationTypeRequest{
			RelationType: &m,
		})
		if err != nil {
			return err
		}

		fmt.Println("relation_type", resp.Result.Id)
	}
	return nil
}

func (c *Client) loadObjects(ctx context.Context, r io.Reader) error {
	objects, _ := js.NewArrayReader(r)
	defer objects.Close()

	var m dsc.Object
	for {
		err := objects.Read(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		resp, err := c.Writer.SetObject(ctx, &dsw.SetObjectRequest{
			Object: &m,
		})
		if err != nil {
			return err
		}

		fmt.Println("object", resp.Result.Id)
	}
	return nil
}

func (c *Client) loadRelations(ctx context.Context, r io.Reader) error {
	relations, _ := js.NewArrayReader(r)
	defer relations.Close()

	var m dsc.Relation
	for {
		err := relations.Read(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		resp, err := c.Writer.SetRelation(ctx, &dsw.SetRelationRequest{
			Relation: &m,
		})
		if err != nil {
			return err
		}

		if err != nil {
			return err
		}
		fmt.Printf("relation %v\n", resp.Result)
	}
	return nil
}
