#!/bin/sh

##H Usage:
##H   cms_site_catalogue -c mysite.config /foo/digi/foobar /foo/hit/bar /mu/hit/mbforpu

config= out=inputurl_orcarc
while [ $# -ge 1 ]; do
  case $1 in
    -c ) shift; config="$1"; shift ;;
    -o ) shift; out="$1"; shift ;;
    -* ) echo "unrecognised option $1" 1>&2; exit 5 ;;
    *  ) break ;;
  esac
done

[ -z "$config" ] && { echo "missing site configuration file" 1>&2; exit 1; }
[ -r "$config" ] || { echo "$config: no such file" 1>&2; exit 1; }
. "$config" || { echo "$config: failed to source" 1>&2; exit 1; }

catalogues=
for arg; do
  tier="$(expr "$arg" : '/[^/]*/\([^/]*\)/[^/]*')"
  dataset="$(expr "$arg" : '/\([^/]*\)/[^/]*/[^/]*')"
  owner="$(expr "$arg" : '/[^/]*/[^/]*/\([^/]*\)')"

  [ -z "$tier" ] && { echo "no datatier found in $arg" 1>&2; exit 1; }
  [ -z "$dataset" ] && { echo "no dataset found in $arg" 1>&2; exit 1; }
  [ -z "$owner" ] && { echo "no owner found in $arg" 1>&2; exit 1; }

  for cat in "$CMS_CATALOGUE_PREFIX/$dataset/POOL_Catalog.META.$dataset.xml" \
             "$CMS_CATALOGUE_PREFIX/$dataset/POOL_Catalog.EVD.$owner.$dataset.xml"; do
    case $catalogues in *" $cat "* ) ;; * ) catalogues="$catalogues $cat ";; esac
  done
done

final_catalogues=
for cat in $catalogues; do
  case $cat in
    rfio:* )
      catfile="$(echo "$cat" | sed 's/^[a-z]*://')"
      local="$(echo "$cat" | sed 's|.*/||')"
      rfcp "$catfile" "$local" || exit $?
      cat="xmlcatalog_file:$PWD/$local"
      ;;

    dcap:* )
      case $cat in
        dcap://* ) catfile="$cat";;
	* )        catfile="$(echo "$cat" | sed 's/^[a-z]*://')" ;;
      esac
      local="$(echo "$cat" | sed 's|.*/||')"
      dccp "$catfile" "$local" || exit $?
      cat="xmlcatalog_file:$PWD/$local"
      ;;

    http:* | file:* )
      cat="xmlcatalog_$cat"
      ;;

    * )
      echo "sorry, catalogue $cat not supported" 1>&2
      exit 1
      ;;
  esac
  final_catalogues="$final_catalogues \n $cat"
done

echo "# Start of site local generated orcarc fragment" > "$out"
echo -e "InputFileCatalogURL = @{ $final_catalogues \n }@ " >> "$out"
echo "# End of site local generated orcarc fragment" >> "$out"

